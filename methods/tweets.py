import datetime
from collections import OrderedDict


class LastTweetsDict(OrderedDict):
    def __init__(self, memory_days, *args, **kwargs):
        self.memory = datetime.timedelta(days=memory_days)
        OrderedDict.__init__(self, *args, **kwargs)

    def delete_older_than(self, del_value):
        del_till_index = next((i for i, value in enumerate(self.values()) if value >= del_value), None)
        if del_till_index is None:
            self.clear()
        else:
            for _ in range(del_till_index):
                self.popitem(last=False)

    def move_to_front(self, key, new_value):
        del self[key]
        OrderedDict.__setitem__(self, key, new_value)

    def similar_to(self, text):
        if text in reversed(list(self.keys())):
            return text
        else:
            for text_in_dict in self:
                similarity = len(text & text_in_dict)
                if similarity >= 6 or similarity == len(text):
                    return text_in_dict
            else:
                return False

    def __setitem__(self, key, value):
        OrderedDict.__setitem__(self, key, value)
        self.delete_older_than(value - self.memory)
