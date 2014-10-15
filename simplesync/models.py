# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

logger = logging.getLogger(__name__)

import json

import django
from django.db import models
from django.db.models import signals
from django.core.serializers import serialize, deserialize
from django.core.serializers.json import DateTimeAwareJSONEncoder

class ModelSyncer(object):

    def __init__(self, model):
        self.model = model

    def get_model_name(self, model_cls):
        if django.VERSION < (1,6):
            return model_cls._meta.module_name
        else:
            return model_cls._meta.model_name

    def post_save_handler(self, sender=None, instance=None, created=None,
                          raw=None, using=None, update_fields=None, **kwargs):
        from . import tasks
        if raw:
            logger.warning('Received "raw" save request for %s %s - declining '
                           'to operate', self.get_model_name(sender), instance.pk)
            return
        if created:
            if not self.can_create(instance):
                logger.debug('Received create signal for %s %s - but not '
                             'authorized by can_create',
                             self.get_model_name(sender), instance.pk)
                return
            result = tasks.do_sync.delay('create',
                                         sender._meta.app_label,
                                         self.get_model_name(sender),
                                         self.to_json(instance))
            logger.info('CREATE - %s %s - queued as %s',
                        self.get_model_name(sender), instance.pk, result.id)
            return
        else:
            if not self.can_update(instance):
                logger.debug('Received update signal for %s %s - but not '
                             'authorized by can_update',
                             self.get_model_name(sender), instance.pk)
                return
            result = tasks.do_sync.delay('update',
                                         sender._meta.app_label,
                                         self.get_model_name(sender),
                                         self.to_json(instance))
            logger.info('UPDATE - %s %s - queued as %s',
                        self.get_model_name(sender), instance.pk, result.id)
            return

    def post_delete_handler(self, sender=None, instance=None, using=None,
                            **kwargs):
        from . import tasks
        if not self.can_delete(instance):
            logger.debug('Received delete signal for %s %s - but not '
                         'authorized by can_delete',
                         self.get_model_name(sender), instance.pk)
            return
        if hasattr(instance, 'natural_key') and \
                hasattr(instance._default_manager, 'get_by_natural_key'):
            json_body = {'pk': instance.natural_key()}
        else:
            attname = 'pk'
            value = instance.pk
            json_body = {attname: value}
        result = tasks.do_sync.delay(
            'delete', sender._meta.app_label, self.get_model_name(sender),
            json.dumps(json_body, cls=DateTimeAwareJSONEncoder))
        logger.info('DELETE - %s %s - queued as %s',
                    self.get_model_name(sender), json_body, result.id)

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
                    **{self.get_model_name(type(instance)): instance,
                       self.get_model_name(model): pk}
                )
                # We need a JSON generator with this model now...
                syncer = type(self)(ThroughClass)
                result = tasks.do_sync.delay('create',
                                             sender._meta.app_label,
                                             self.get_model_name(sender),
                                             syncer.to_json(obj))
                logger.info('CREATE - %s %s - queued as %s',
                            self.get_model_name(sender), instance.pk, result.id)
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
                        json_body[self.get_model_name(type(obj))] = obj.natural_key()
                    else:
                        attname = 'pk'
                        value = obj.pk
                        json_body['%s__%s' % self.get_model_name((type(obj)),
                                              attname)] = value
            result = tasks.do_sync.delay(
                'delete', sender._meta.app_label, self.get_model_name(sender),
                json.dumps(json_body, cls=DateTimeAwareJSONEncoder))
            logger.info('DELETE - %s %s - queued as %s',
                        self.get_model_name(sender), json_body, result.id)
        if action == 'post_clear' and \
                self.can_remove_m2m(type(instance), model):
            if hasattr(type(instance), 'natural_key') and \
                    hasattr(type(instance)._default_manager,
                            'get_by_natural_key'):
                json_body[self.get_model_name(type(instance))] = instance.natural_key()
            else:
                attname = 'pk'
                value = instance.pk
                json_body = {attname: value}
            result = tasks.do_sync.delay(
                'delete', sender._meta.app_label, self.get_model_name(sender),
                json.dumps(json_body, cls=DateTimeAwareJSONEncoder))
            logger.info('DELETE - %s %s - queued as %s',
                        self.get_model_name(sender), json_body, result.id)

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
        return serialize('json', [obj], use_natural_keys=True)

    def from_json(self, json_obj):
        logger.debug('json_obj: %s', json_obj)
        deserialized_obj = deserialize('json', json_obj).next()
        return deserialized_obj.object, deserialized_obj.m2m_data

class SyncerRegistry(object):
    def __init__(self):
        self.registered = {}

    def register(self, model, cls):
        if model in self.registered:
            logger.info('Model %s is already registered for syncing',
                        self.get_model_name(model))
            return
        instance = cls(model)
        signals.post_save.connect(instance.post_save_handler, sender=model)
        signals.post_delete.connect(instance.post_delete_handler,
                                    sender=model)
        signals.m2m_changed.connect(instance.m2m_changed_handler)
        self.registered[model] = instance

__registry__ = SyncerRegistry()
is_registered = lambda model_cls: model_cls in __registry__.registered
