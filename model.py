import biblioteca_model as bmodel
import dataset


class Model(object):

    def __init__(self, application_items):
        self._item_type = 'product'
        self._connection = dataset.connect('sqlite:///mydatabase.db')
        bmodel.create_table(self._connection, self._item_type)
        self.create_items(application_items)

    @property
    def item_type(self):
        return self._item_type

    @item_type.setter
    def item_type(self, new_item_type):
        self._item_type = new_item_type

    @property
    def connection(self):
        return self._connection

    def create_item(self, name, price, quantity):
        bmodel.insert_one(
            self.connection, name, price, quantity, table_name=self.item_type)

    def create_items(self, items):
        bmodel.insert_many(
            self.connection, items, table_name=self.item_type)

    def read_item(self, name):
        return bmodel.select_one(
            self.connection, name, table_name=self.item_type)

    def read_items(self):
        return bmodel.select_all(
            self.connection, table_name=self.item_type)

    def update_item(self, name, price, quantity):
        bmodel.update_one(
            self.connection, name, price, quantity, table_name=self.item_type)

    def delete_item(self, name):
        bmodel.delete_one(
            self.connection, name, table_name=self.item_type)