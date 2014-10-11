# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import json

from django.db import models
from django.db.models import signals
from django.core.serializers import serialize, deserialize
from django.core.serializers.json import DateTimeAwareJSONEncoder

class ModelSyncer(object):

    def __init__(self, model):
        self.model = model

    def post_save_handler(self, sender=None, instance=None, created=None,
                          raw=None, using=None, update_fields=None, **kwargs):
        from . import tasks
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
                                         self.to_json(instance))
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
                                         self.to_json(instance))
            logger.info('UPDATE - %s %s - queued as %s',
                        sender._meta.model_name, instance.pk, result.id)
            return

    def post_delete_handler(self, sender=None, instance=None, using=None,
                            **kwargs):
        from . import tasks
        if not self.can_delete(instance):
            logger.debug('Received delete signal for %s %s - but not '
                         'authorized by can_delete',
                         sender._meta.model_name, instance.pk)
            return
        if hasattr(sender, 'natural_key') and \
                hasattr(sender._default_manager, 'get_by_natural_key'):
            json_body = {'pk': sender.natural_key()}
        else:
            attname = 'pk'
            value = instance.pk
            json_body = {attname: value}
        result = tasks.do_sync.delay(
            'delete', sender._meta.app_label, sender._meta.model_name,
            json.dumps(json_body))
        logger.info('DELETE - %s %s - queued as %s',
                    sender._meta.model_name, json_body, result.id)

    def m2m_changed_handler(self, sender=None, instance=None, action=None,
                            reverse=None, model=None, pk_set=None,
                            using=None, **kwargs):
        if model != self.model:
            return
        from . import tasks
        if action == 'post_add' and self.can_add_m2m(type(instance), model):
            # Treat this like a create
            for pk in pk_set:
                ThroughClass = sender
                obj = ThroughClass.objects.get(
                    **{type(instance)._meta.model_name: instance,
                       model._meta.model_name: pk}
                )
                # We need a JSON generator with this model now...
                syncer = type(self)(ThroughClass)
                result = tasks.do_sync.delay('create',
                                             sender._meta.app_label,
                                             sender._meta.model_name,
                                             syncer.to_json(obj))
                logger.info('CREATE - %s %s - queued as %s',
                            sender._meta.model_name, instance.pk, result.id)
            return
        if action == 'post_remove' and \
                self.can_remove_m2m(type(instance), model):
            for pk in pk_set:
                json_body = {}
                other_instance = model.objects.get(pk=pk)
                for obj in [instance, other_instance]:
                    if hasattr(type(obj), 'natural_key') and \
                            hasattr(type(obj)._default_manager,
                                    'get_by_natural_key'):
                        json_body[type(obj)._meta.model_name] = obj.natural_key()
                    else:
                        attname = 'pk'
                        value = obj.pk
                        json_body['%s__%s' % (type(obj)._meta.model_name,
                                              attname)] = value
            result = tasks.do_sync.delay(
                'delete', sender._meta.app_label, sender._meta.model_name,
                json.dumps(json_body, cls=DateTimeAwareJSONEncoder))
            logger.info('DELETE - %s %s - queued as %s',
                        sender._meta.model_name, json_body, result.id)
        if action == 'post_clear' and \
                self.can_remove_m2m(type(instance), model):
            if hasattr(type(instance), 'natural_key') and \
                    hasattr(type(instance)._default_manager,
                            'get_by_natural_key'):
                json_body[type(instance)._meta.model_name] = instance.natural_key()
            else:
                attname = 'pk'
                value = instance.pk
                json_body = {attname: value}
            result = tasks.do_sync.delay(
                'delete', sender._meta.app_label, sender._meta.model_name,
                json.dumps(json_body, cls=DateTimeAwareJSONEncoder))
            logger.info('DELETE - %s %s - queued as %s',
                        sender._meta.model_name, json_body, result.id)

    def can_add_m2m(self, model, other_model):
        return is_registered(model) and is_registered(other_model)

    def can_remove_m2m(self, model, other_model):
        return is_registered(model) and is_registered(other_model)

    def can_create(self, obj):
        return True

    def can_update(self, obj):
        return True

    def can_delete(self, obj):
        return True

    def to_json(self, obj):
        return serialize('json', [obj], use_natural_keys=True)[0]

        json_obj = {}
        for field in self.model._meta.concrete_fields:
            # if the related object has a SlugField, use that versus the
            # numeric ID
            if field.rel:
                rel_obj = getattr(obj, field.name)
                if hasattr(type(rel_obj)._default_manager,
                           'get_by_natural_key') and \
                        hasattr(rel_obj, 'natural_key'):
                    json_obj[field.attname] = rel_obj.natural_key()
                else:
                    json_obj[field.attname] = getattr(obj, field.attname)
            elif field.primary_key and \
                    hasattr(self.model._default_manager,
                            'get_by_natural_key') and \
                    hasattr(self.model, 'natural_key'):
                json_obj['pk'] = obj.natural_key()
            else:
                json_obj[field.attname] = getattr(obj, field.attname)

        # now handle m2m fields
        for field in self.model._meta.many_to_many:
            rel_mgr = getattr(obj, field.name)
            rel_model = field.rel.to
            if hasattr(rel_model._default_manager, 'get_by_natural_key') and \
                    hasattr(rel_model, 'natural_key'):
                json_obj[field.attname] = [rel_obj.natural_key()
                                           for rel_obj in rel_mgr.all()]
            else:
                json_obj[field.attname] = [
                    rel_obj.pk for rel_obj in rel_mgr.all()]
        return json_obj

    def __decode_rel_value__(self, obj, field, value):
        if hasattr(value, '__iter__') and \
                hasattr(field.rel.to._default_manager,
                        'get_by_natural_key') and \
                hasattr(field.rel.to, 'natural_key'):
            # I'll bet you can guess this is a natural key.
            try:
                rel_obj = field.rel.to._default_manager.get_by_natural_key(
                    *value)
            except field.rel.to.DoesNotExist:
                logger.warning('Relation DNE: %s -> %s=%s', obj, field.rel.to, value)
                raise
            else:
                return rel_obj

        if isinstance(value, (int, long)):
            # This is the primary key of the related object.
            try:
                rel_obj = field.rel.to.objects.get(pk=value)
            except field.rel.to.DoesNotExist:
                logger.warning('Relation DNE: %s -> %s=%s', obj, field.rel.to, value)
                raise
            else:
                return rel_obj

        # Uh. Maybe the other model has a CharField for a pk??
        try:
            rel_obj = field.rel.to.objects.get(pk=value)
        except ValueError:
            # Nope.
            logger.warning('Got a non-integer value for '
                           'related field %s to %s but the primary '
                           'key is not a string type.',
                           field.attname,
                           field.rel.to._meta.model_name)
            raise models.ObjectDoesNotExist
        else:
            # Sure. Why not.
            return rel_obj

    def from_json(self, json_obj):
        return deserialize('json', [json_obj])
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
                elif field_name == 'pk':
                    field = self.model._meta.pk
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
            elif field.primary_key and \
                    hasattr(self.model._default_manager,
                            'get_by_natural_key') and \
                    hasattr(self.model, 'natural_key'):
                try:
                    obj.pk = self.model._default_manager.get_by_natural_key(*value)
                except self.model.DoesNotExist:
                    obj.pk = None
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
        signals.m2m_changed.connect(instance.m2m_changed_handler)
        self.registered[model] = instance

__registry__ = SyncerRegistry()
is_registered = lambda model_cls: model_cls in __registry__.registered
