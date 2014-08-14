from django.db import models

class CharFieldNaturalKeyManager(models.Manager):
    def get_by_natural_key(self, char_field):
        return self.get(char_field=char_field)

class RelatedModel(models.Model):
    char_field = models.CharField(max_length=20, unique=True)

    def natural_key(self):
        return self.char_field,

    objects = CharFieldNaturalKeyManager()

class RelatedModelWithSlug(models.Model):
    slug_field = models.SlugField()
    char_field = models.CharField(max_length=20)

class M2MRelatedModel(models.Model):
    char_field = models.CharField(max_length=20)

class SlugFieldNaturalKeyManager(models.Manager):
    def get_by_natural_key(self, slug_field):
        return self.get(slug_field=slug_field)

class M2MRelatedModelWithSlug(models.Model):
    slug_field = models.SlugField(unique=True)
    char_field = models.CharField(max_length=20)

    def natural_key(self):
        return self.slug_field,

    objects = SlugFieldNaturalKeyManager()

class TestModel(models.Model):
    char_field = models.CharField(max_length=20)
    int_field = models.IntegerField()
    datetime_field = models.DateTimeField()
    fk_field = models.ForeignKey(RelatedModel)
    m2m_field = models.ManyToManyField(M2MRelatedModel)
    fk_slug_field = models.ForeignKey(RelatedModelWithSlug)
    m2m_slug_field = models.ManyToManyField(M2MRelatedModelWithSlug)

class ReverseRelationModel(models.Model):
    fk_field = models.ForeignKey(TestModel)

class ReverseM2MRelationModel(models.Model):
    m2m_field = models.ManyToManyField(TestModel)

from simplesync import register
from django.conf import settings

if settings.DO_SYNC:
    register(RelatedModel)
    register(RelatedModelWithSlug)
    register(M2MRelatedModel)
    register(M2MRelatedModelWithSlug)
    register(TestModel)
