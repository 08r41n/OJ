import fnmatch
import json
import logging
import os
import re
import zipfile
import tempfile, subprocess, io
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from celery import shared_task
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.files.storage import default_storage
from django.utils.translation import gettext as _
from moss import MOSS

from judge.models import Contest, ContestMoss, ContestParticipation, ContestSubmission, Problem, Submission
from judge.utils.celery import Progress

from django.utils import timezone

__all__ = ('rescore_contest', 'run_moss', 'prepare_contest_data')
rewildcard = re.compile(r'\*+')


@shared_task(bind=True)
def rescore_contest(self, contest_key):
    contest = Contest.objects.get(key=contest_key)
    participations = contest.users

    rescored = 0
    with Progress(self, participations.count(), stage=_('Recalculating contest scores')) as p:
        for participation in participations.iterator():
            participation.recompute_results()
            rescored += 1
            if rescored % 10 == 0:
                p.done = rescored
    return rescored


@shared_task(bind=True)
def run_moss(self, contest_key):
    moss_api_key = settings.MOSS_API_KEY
    if moss_api_key is None:
        raise ImproperlyConfigured('No MOSS API Key supplied')

    contest = Contest.objects.get(key=contest_key)
    ContestMoss.objects.filter(contest=contest).delete()

    length = len(ContestMoss.LANG_MAPPING) * contest.problems.count()
    moss_results = []

    with Progress(self, length, stage=_('Running MOSS')) as p:
        for problem in contest.problems.all():
            for dmoj_lang, moss_lang in ContestMoss.LANG_MAPPING:
                result = ContestMoss(contest=contest, problem=problem, language=dmoj_lang)

                subs = Submission.objects.filter(
                    contest__participation__virtual__in=(ContestParticipation.LIVE, ContestParticipation.SPECTATE),
                    contest_object=contest,
                    problem=problem,
                    language__common_name=dmoj_lang,
                ).order_by('-points').values_list('user__user__username', 'source__source')

                if subs.exists():
                    moss_call = MOSS(moss_api_key, language=moss_lang, matching_file_limit=100,
                                     comment='%s - %s' % (contest.key, problem.code))

                    users = set()

                    for username, source in subs:
                        if username in users:
                            continue
                        users.add(username)
                        moss_call.add_file_from_memory(username, source.encode('utf-8'))

                    result.url = moss_call.process()
                    result.submission_count = len(users)

                moss_results.append(result)
                p.did(1)

    ContestMoss.objects.bulk_create(moss_results)

    return len(moss_results)


@shared_task(bind=True)
def prepare_contest_data(self, contest_id, options):
    options = json.loads(options)

    with Progress(self, 1, stage=_('Applying filters')) as p:
        # Force an update so that we get a progress bar.
        p.done = 0
        contest = Contest.objects.get(id=contest_id)
        queryset = ContestSubmission.objects.filter(participation__contest=contest, participation__virtual=0) \
                                    .order_by('-points', 'id') \
                                    .select_related('problem__problem', 'submission__user__user',
                                                    'submission__source', 'submission__language') \
                                    .values_list('submission__user__user__id', 'submission__user__user__username',
                                                 'problem__problem__code', 'submission__source__source',
                                                 'submission__language__extension', 'submission__id',
                                                 'submission__language__file_only')

        if options['submission_results']:
            queryset = queryset.filter(result__in=options['submission_results'])

        # Compress wildcards to avoid exponential complexity on certain glob patterns before Python 3.9.
        # For details, see <https://bugs.python.org/issue40480>.
        problem_glob = rewildcard.sub('*', options['submission_problem_glob'])
        if problem_glob != '*':
            queryset = queryset.filter(
                problem__problem__in=Problem.objects.filter(code__regex=fnmatch.translate(problem_glob)),
            )

        submissions = list(queryset)
        p.did(1)

    length = len(submissions)
    with Progress(self, length, stage=_('Preparing contest data')) as p:
        data_file = zipfile.ZipFile(os.path.join(settings.DMOJ_CONTEST_DATA_CACHE, '%s.zip' % contest_id), mode='w')
        exported = set()
        for user_id, username, problem, source, ext, sub_id, file_only in submissions:
            if (user_id, problem) in exported:
                path = os.path.join(username, '$History', f'{problem}_{sub_id}.{ext}')
            else:
                path = os.path.join(username, f'{problem}.{ext}')
                exported.add((user_id, problem))

            if file_only:
                # Get the basename of the source as it is an URL
                filename = os.path.basename(source)
                data_file.write(
                    default_storage.path(os.path.join(settings.SUBMISSION_FILE_UPLOAD_MEDIA_DIR,
                                         problem, str(user_id), filename)),
                    path,
                )
                pass
            else:
                data_file.writestr(path, source)

            p.did(1)

        data_file.close()

    return length

@shared_task
def schedule_auto_export(contest_id):
    try:
        contest = Contest.objects.get(id=contest_id)
        # nếu contest còn chưa kết thúc thì lên lịch
        if contest.end_time > timezone.now():
            print(f"[SCHEDULE] Contest {contest} chưa kết thúc. Lên lịch export vào {contest.end_time}")
            export_contest_to_drive.apply_async((contest_id,), eta=contest.end_time)
        else:
            # nếu đã kết thúc thì export ngay lập tức
            export_contest_to_drive.delay(contest_id)
    except Contest.DoesNotExist:
        print(f"[SCHEDULE] Contest {contest_id} không tồn tại")
        return

@shared_task
def export_contest_to_drive(contest_id):
    try:
        contest = Contest.objects.get(id=contest_id)
    except Contest.DoesNotExist:
        print(f"[EXPORT] Contest {contest_id} không tồn tại")
        return None

    db_settings = settings.DATABASES['default']
    db_user = db_settings['USER']
    db_password = db_settings.get('PASSWORD', '')
    db_name = db_settings['NAME']
    db_host = db_settings.get('HOST', 'localhost')
    db_port = str(db_settings.get('PORT', '3306'))

    with tempfile.TemporaryDirectory() as tmpdir:
        sql_paths = []
        contest_id = contest.id

        sql_filename = f'{contest}.sql'
        sql_filepath = os.path.join(tmpdir, sql_filename)

        tables = [
            'judge_contest',
            'judge_contestproblem',
            'judge_contestannouncement',
            'judge_contestmoss',
            'judge_contestparticipation',
            'judge_contestsubmission',
            'judge_submission',
            'judge_submissionsource',
            'judge_submissiontestcase',
            'auth_user',
            'judge_profile',
            'judge_problem',
            # Many-to-many
            'judge_problem_allowed_languages',
            'judge_languagelimit',
            'judge_problem_authors',
            'judge_problem_banned_users',
            'judge_problem_curators',
            'judge_problem_organizations',
            'judge_problem_testers',
            'judge_problem_types',
            'judge_problemclarification',
            'judge_problemdata',
            'judge_problemgroup',
            'judge_problemtestcase',
            'judge_problemtranslation',
            'judge_problemtype',
            'judge_contest_authors',
            'judge_contest_curators',
            'judge_contest_testers',
            'judge_contest_tags',
            'judge_contest_private_contestants',
            'judge_contest_organizations',
            'judge_contest_banned_users',
            'judge_contest_banned_judges',
            'judge_contest_view_contest_scoreboard',
            'judge_contest_rate_exclude',
            'judge_examaccess',
        ]

        with open(sql_filepath, 'w') as f:
            f.write(f"-- SQL dump for contest {contest_id}\n")
            f.write("SET FOREIGN_KEY_CHECKS=0;\n\n")

        dump_env = os.environ.copy()
        if db_password:
            dump_env['MYSQL_PWD'] = db_password

        has_data = False

        for table in tables:
            # mapping điều kiện where
            if table == 'judge_contest':
                condition = f"id={contest_id}"
            elif table == 'judge_contestproblem':
                condition = f"contest_id={contest_id}"
            elif table.startswith('judge_contest') or table == 'judge_examaccess':
                condition = f"contest_id={contest_id}"
            elif table == 'judge_contestsubmission':
                condition = f"participation_id IN (SELECT id FROM judge_contestparticipation WHERE contest_id={contest_id})"
            elif table == 'judge_submission':
                condition = f"id IN (SELECT submission_id FROM judge_contestsubmission WHERE participation_id IN (SELECT id FROM judge_contestparticipation WHERE contest_id={contest_id}))"
            elif table == 'judge_submissionsource':
                condition = f"submission_id IN (SELECT submission_id FROM judge_contestsubmission WHERE participation_id IN (SELECT id FROM judge_contestparticipation WHERE contest_id={contest_id}))"
            elif table == 'judge_submissiontestcase':
                condition = f"submission_id IN (SELECT submission_id FROM judge_contestsubmission WHERE participation_id IN (SELECT id FROM judge_contestparticipation WHERE contest_id={contest_id}))"
            elif table == 'auth_user':
                condition = f"id IN (SELECT user_id FROM judge_profile WHERE id IN (SELECT profile_id FROM judge_contest_private_contestants WHERE contest_id={contest_id}))"
            elif table == 'judge_profile':
                condition = f"id IN (SELECT profile_id FROM judge_contest_private_contestants WHERE contest_id={contest_id})"
            elif table == 'judge_problem':
                condition = f"id IN (SELECT problem_id FROM judge_contestproblem WHERE contest_id={contest_id})"
            elif table == 'judge_problemdata':
                condition = f"problem_id IN (SELECT problem_id FROM judge_contestproblem WHERE contest_id={contest_id})"
            elif table == 'judge_problemtestcase':
                condition = f"dataset_id IN (SELECT problem_id FROM judge_contestproblem WHERE contest_id={contest_id})"
            elif table == 'judge_languagelimit':
                condition = f"problem_id IN (SELECT problem_id FROM judge_contestproblem WHERE contest_id={contest_id})"
            elif table.startswith('judge_problem_'):
                condition = f"problem_id IN (SELECT problem_id FROM judge_contestproblem WHERE contest_id={contest_id})"
            else:
                continue

            cmd = [
                'mysqldump',
                f'--host={db_host}',
                f'--port={db_port}',
                f'--user={db_user}',
                '--skip-extended-insert',
                '--no-create-info',
                '--no-create-db',
                '--skip-triggers',
                '--single-transaction',
                db_name,
                '--where', condition,
                table
            ]

            temp_file = os.path.join(tmpdir, f"{table}_{contest}.sql")
            result = subprocess.run(
                cmd, stdout=open(temp_file, 'w'),
                stderr=subprocess.PIPE, env=dump_env, text=True
            )
            if result.returncode == 0:
                with open(temp_file) as tf:
                    content = tf.read()
                    if re.search(r'INSERT\s+INTO', content, re.IGNORECASE):
                        with open(sql_filepath, 'a') as mainf:
                            mainf.write(content + "\n")
                        has_data = True
            os.remove(temp_file)

        with open(sql_filepath, 'a') as f:
            f.write("\nSET FOREIGN_KEY_CHECKS=1;\n")

        if not has_data:
            return None

        date_str = datetime.now().strftime('%Y%m%d')
        zip_filename = f"{date_str}_{contest}.zip"
        zip_filepath = os.path.join(tmpdir, zip_filename)
        with zipfile.ZipFile(zip_filepath, 'w') as zipf:
            zipf.write(sql_filepath, arcname=os.path.basename(sql_filepath))

        gauth = GoogleAuth(settings_file="config/drive/settings.yaml")
        drive = GoogleDrive(gauth)

        folder = settings.DRIVE_FOLDER_ID
        gfile = drive.CreateFile({
            'parents': [{'id': folder}],
            'title': os.path.basename(zip_filepath)  
        })
        gfile.SetContentFile(zip_filepath)
        gfile.Upload()

        print(f"Uploaded {zip_filepath} to Google Drive folder {folder}")