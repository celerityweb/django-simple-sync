# These are non-standard tests, because it requires a celery worker to be
# running to ensure the data copies.

# So run this script from a manage.py shell while another process runs
# manage.py celery worker -l DEBUG --settings=test_project.other_settings

import time
from django.utils.timezone import now

from .models import *

def test_script():
    # Adds

    # Expecting create event
    rm = RelatedModel.objects.create(char_field='foo')

    # Expecting create event
    rms = RelatedModelWithSlug.objects.create(char_field='foo',
                                              slug_field='bar')

    # Expecting create event
    m2mrm = M2MRelatedModel.objects.create(char_field='foo')

    # Expecting create event
    m2mrm2 = M2MRelatedModel.objects.create(char_field='bar')

    # Expecting create event
    m2mrms = M2MRelatedModelWithSlug.objects.create(char_field='foo',
                                                    slug_field='foo')

    # Expecting create event
    m2mrms2 = M2MRelatedModelWithSlug.objects.create(char_field='bar',
                                                     slug_field='bar')

    # Expecting create event
    tm = TestModel.objects.create(char_field='foo',
                                  int_field=5,
                                  datetime_field=now(),
                                  fk_field=rm,
                                  fk_slug_field=rms)

    # Expecting create event
    tm.m2m_field.add(m2mrm)

    # Expecting create event
    tm.m2m_slug_field.add(m2mrms)

    # Expecting create event
    m2mrm2.testmodel_set.add(tm)

    # Expecting create event
    m2mrms2.testmodel_set.add(tm)

    # Should not sync
    rrm = ReverseRelationModel.objects.create(fk_field=tm)
    rm2mrm = ReverseM2MRelationModel.objects.create()
    rm2mrm.m2m_field.add(tm)

    # Changes
    tm.char_field = 'bar'
    tm.int_field = 6
    tm.datetime_field = now()
    # Expecting update
    tm.save()

    # Deletes
    tm.m2m_field.clear()
    m2mrms.testmodel_set.remove(tm)
    m2mrms.delete()

    time.sleep(2)
    # tm.delete()
