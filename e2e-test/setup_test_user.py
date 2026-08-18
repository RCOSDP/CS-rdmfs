import os

from django.utils import timezone
from osf.models import Institution, OSFUser


username = os.environ['RDMFS_E2E_USERNAME']
password = os.environ['RDMFS_E2E_PASSWORD']
institution_name = os.environ['RDMFS_E2E_INSTITUTION']

assert not OSFUser.objects.filter(username=username).exists(), (
    f'The dedicated E2E user already exists: {username}'
)
user = OSFUser(
    username=username,
    fullname='RDMFS E2E User',
    given_name='RDMFS',
    family_name='E2E User',
    given_name_ja='RDMFS',
    family_name_ja='E2Eユーザー',
    is_active=True,
    date_registered=timezone.now(),
)
user.set_password(password)
user.save()
user.is_registered = True
user.date_confirmed = timezone.now()
user.have_email = True
user.jobs = [{
    'institution': institution_name,
    'department': None,
    'institution_ja': institution_name,
    'department_ja': None,
    'title': None,
    'startMonth': None,
    'startYear': None,
    'endMonth': None,
    'endYear': None,
    'ongoing': None,
}]
user.save()
user.emails.create(address=username)
print(f'Created test user without a project: {username}')

institution = Institution.objects.get(name=institution_name)
user.affiliated_institutions.add(institution)
user.save()
print(f'Affiliated {username} with {institution_name}')

assert not user.nodes.filter(category='project').exists(), (
    f'The dedicated E2E user unexpectedly has a project: {username}'
)
