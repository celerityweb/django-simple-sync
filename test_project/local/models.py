from django.db import models

class RelatedModel(models.Model):
    char_field = models.CharField(max_length=20)

class RelatedModelWithSlug(models.Model):
    slug_field = models.SlugField()
    char_field = models.CharField(max_length=20)

class M2MRelatedModel(models.Model):
    char_field = models.CharField(max_length=20)

class M2MRelatedModelWithSlug(models.Model):
    slug_field = models.SlugField()
    char_field = models.CharField(max_length=20)

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
