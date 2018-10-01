class Action(object):
    def __init__(self, member):
        self.member = member

    def serialize(self):
        d = {'id': self.member.unique_id,
             'source': self.member.source_name,
             'action': self.action_name()}
        d.update(self.additional_fields())
        return d

    def additional_fields(self):
        return {}

class CreateAction(Action):
    def action_name(self):
        return "create"

class DeleteAction(Action):
    def action_name(self):
        return "delete"

class UpdateAction(Action):
    def action_name(self):
        return "update"

    def additional_fields(self):
        d = {}
        for key in self.member.dirty_fields():
            d[key] = self.member.get(key)
        return d

