# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import json

from django.db import models
from django.db.models import signals
from django.core.serializers.json import DateTimeAwareJSONEncoder

from . import tasks


class ModelSyncer(object):

    def __init__(self, model):
        self.model = model

    def post_save_handler(self, sender=None, instance=None, created=None,
                          raw=None, using=None, update_fields=None, **kwargs):
        if raw:
            logger.warning('Received "raw" save request for %s %s - declining '
                           'to operate', sender._meta.model_name, instance.pk)
            return
        if created:
            if not self.can_create(instance):
                logger.debug('Received create signal for %s %s - but not '
                             'authorized by can_create',
                             sender._meta.model_name, instance.pk)
                return
            result = tasks.do_sync.delay('create',
                                         sender._meta.app_label,
                                         sender._meta.model_name,
                                         json.dumps(
                                             self.to_json(instance),
                                             cls=DateTimeAwareJSONEncoder))
            logger.info('CREATE - %s %s - queued as %s',
                        sender._meta.model_name, instance.pk, result.id)
            return
        else:
            if not self.can_update(instance):
                logger.debug('Received update signal for %s %s - but not '
                             'authorized by can_update',
                             sender._meta.model_name, instance.pk)
                return
            result = tasks.do_sync.delay('update',
                                         sender._meta.app_label,
                                         sender._meta.model_name,
                                         json.dumps(
                                             self.to_json(instance),
                                             cls=DateTimeAwareJSONEncoder))
            logger.info('UPDATE - %s %s - queued as %s',
                        sender._meta.model_name, instance.pk, result.id)
            return

    def post_delete_handler(self, sender=None, instance=None, using=None,
                            **kwargs):
        if not self.can_delete(instance):
            logger.debug('Received delete signal for %s %s - but not '
                         'authorized by can_delete',
                         sender._meta.model_name, instance.pk)
            return
        slug_field = self.find_slug_field(sender)
        if slug_field:
            attname = slug_field.attname
            value = getattr(instance, slug_field.attname)
        else:
            attname = 'pk'
            value = instance.pk
        result = tasks.do_sync.delay(
            'delete', sender._meta.app_label, sender._meta.model_name,
            json.dumps({attname: value}))
        logger.info('DELETE - %s %s - queued as %s',
                    sender._meta.model_name, value, result.id)

    def can_create(self, obj):
        return True

    def can_update(self, obj):
        return False

    def can_delete(self, obj):
        return True

    def find_slug_field(self, model):
            slug_fields = [
                f for f in model._meta.concrete_fields
                if isinstance(f, models.SlugField)]
            if slug_fields:
                # The first among them is what we'll use
                return slug_fields[0]

    def to_json(self, obj):
        json_obj = {}
        for field in self.model._meta.concrete_fields:
            # if the related object has a SlugField, use that versus the
            # numeric ID
            if field.rel:
                rel_obj = getattr(obj, field.name)
                rel_obj_slug_field = self.find_slug_field(field.rel.to)
                if rel_obj_slug_field:
                    json_obj[field.attname] = getattr(
                        rel_obj, rel_obj_slug_field.attname)
                else:
                    json_obj[field.attname] = getattr(obj, field.attname)
            else:
                json_obj[field.attname] = getattr(obj, field.attname)

        # now handle m2m fields
        for field in self.model._meta.many_to_many:
            rel_mgr = getattr(obj, field.name)
            rel_model = field.rel.to
            rel_obj_slug_field = self.find_slug_field(rel_model)
            if rel_obj_slug_field:
                json_obj[field.attname] = [
                    getattr(rel_obj, rel_obj_slug_field.attname)
                    for rel_obj in rel_mgr.all()]
            else:
                json_obj[field.attname] = [
                    rel_obj.pk for rel_obj in rel_mgr.all()]

        return json_obj

    def __decode_rel_value__(self, obj, field, value):
        if isinstance(value, (int, long)):
            # This is the primary key of the related object.
            try:
                rel_obj = field.rel.to.objects.get(pk=value)
            except field.rel.to.DoesNotExist:
                logger.warning('Could not find a related %s object with a pk '
                               'of %s', field.rel.to._meta.model_name, value)
                return None
            else:
                return rel_obj
        else:
            # This is probably a slug.
            rel_obj_slug_field = self.find_slug_field(field.rel.to)
            if rel_obj_slug_field:
                try:
                    rel_obj = field.rel.to.objects.get(
                        **{rel_obj_slug_field.attname: value}
                    )
                except field.rel.to.DoesNotExist:
                    logger.warning('Could not find related %s object '
                                   'with a %s slug of %s',
                                   field.rel.to._meta.model_name,
                                   rel_obj_slug_field, value)
                    return None
                else:
                    return rel_obj
            else:
                # Uh. Maybe the other model has a CharField for a pk??
                try:
                    rel_obj = field.rel.to.objects.get(pk=value)
                except ValueError:
                    # Nope.
                    logger.warning('Got a non-integer value for '
                                   'related field %s to %s but did not '
                                   'find a slug field and the primary '
                                   'key is not a string type.',
                                   field.attname,
                                   field.rel.to._meta.model_name)
                    return None
                else:
                    # Sure. Why not.
                    return rel_obj


    def from_json(self, json_obj):
        obj = self.model()
        m2m_values = {}
        for field_name, value in json_obj.iteritems():
            try:
                field = self.model._meta.get_field(field_name)
            except models.FieldDoesNotExist:
                if field_name.endswith('_id'):
                    try:
                        field = self.model._meta.get_field(field_name[:-3])
                    except models.FieldDoesNotExist:
                        logger.warning('Could not find field on model for JSON '
                                       'object key %s', field_name)
                        continue
                else:
                    logger.warning('Could not find field on model for JSON '
                                   'object key %s', field_name)
                    continue
            if field.rel:
                # This is a fk relation - we act differently for m2o and m2m
                if hasattr(field.rel, 'through'):
                    # We expect value to be a list.
                    if not isinstance(value, list):
                        logger.warning('Expected a list-like object for %s '
                                       'because it is a m2m relation but %s '
                                       'is not that.', field.attname,
                                       value)
                        continue
                    # Now, we can't actually set anything on the object until
                    # it's saved, so we'll track M2M lists apart from the
                    # object and let the caller of this function decide how and
                    # when to bring them together.
                    m2m_values[field.attname] = filter(
                        None,
                        [self.__decode_rel_value__(obj, field, rel_value)
                         for rel_value in value])
                else:
                    # This is a many-to-one relation, so only one value.
                    setattr(obj, field.attname,
                            getattr(self.__decode_rel_value__(obj,
                                                              field,
                                                              value),
                                    'pk', None))
            else:
                setattr(obj, field.attname, value)
        return obj, m2m_values

class SyncerRegistry(object):
    def __init__(self):
        self.registered = {}

    def register(self, model, cls):
        if model in self.registered:
            logger.info('Model %s is already registered for syncing',
                        model._meta.model_name)
            return
        instance = cls(model)
        signals.post_save.connect(instance.post_save_handler, sender=model)
        signals.post_delete.connect(instance.post_delete_handler,
                                    sender=model)
        self.registered[model] = instance

__registry__ = SyncerRegistry()
