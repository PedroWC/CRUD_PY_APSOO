import dataset
import exceptions as exc
from sqlalchemy.exc import NoSuchTableError, IntegrityError, NoSuchTableError

DB_name = 'myDB'

def create_table(conn, table_name):
    """Carregue uma tabela ou crie-a se ela ainda não existir.

     A função load_table só pode carregar uma tabela se existir e gera um NoSuchTableError se a tabela ainda não existir no banco de dados.

     A função get_table carrega uma tabela ou a cria se ela ainda não existir. A nova tabela terá automaticamente uma coluna id, a menos que seja especificado por meio do parâmetro opcional primary_id, que será usado como a chave primária da tabela.

    Parâmetros
    ----------
    table_name : str
    conn : dataset.persistence.database.Database
    """
    try:
        print(conn.load_table(table_name))
    except Exception as e:
        print('Table {} does not exist. It will be created now'.format(e))
        conn.create_table(table_name, primary_id='name')
        print('Created table {} on database {}'.format(table_name, DB_name))

######################################################
####################### CREATE #######################
######################################################

def insert_one(conn, name, price, quantity, table_name):
    """Insira um único item em uma tabela.

    Parâmetros
    ----------
    name : str
    price : float
    quantity : int
    table_name : dataset.persistence.table.Table
    conn : dataset.persistence.database.Database

    Raises
    ------
    exc.ItemAlreadyStored: Se o registro já estiver armazenado na tabela.
    """
    table = conn.load_table(table_name)
    try:
        table.insert(dict(name=name, price=price, quantity=quantity))
    except IntegrityError as e:
        raise exc.ItemAlreadyStored(
            '"{}" already stored in table "{}".\nOriginal Exception raised: {}'
            .format(name, table.table.name, e))


def insert_many(conn, items, table_name):
    """Insira todos os itens em uma tabela.

    Parâmetros
    ----------
    items : list
        lista de dicionários
    table_name : str
    conn : dataset.persistence.database.Database
    """
    # TODO: verifique o que acontece se 1+ registros podem ser inseridos mas 1 falha
    table = conn.load_table(table_name)
    try:
        for x in items:
            table.insert(dict(
                name=x['name'], price=x['price'], quantity=x['quantity']))
    except IntegrityError as e:
        print('At least one in {} was already stored in table "{}".\nOriginal '
              'Exception raised: {}'
              .format([x['name'] for x in items], table.table.name, e))

####################################################
####################### READ #######################
####################################################

def select_one(conn, name, table_name):
    """Selecione um único item em uma tabela.

    A biblioteca do conjunto de dados retorna um resultado como um OrderedDict.

    Parâmetros
    ----------
    name : str
        nome do registro a ser procurado na tabela
    table_name : str
    conn : dataset.persistence.database.Database

    Raises
    ------
    exc.ItemNotStored: se o registro não estiver armazenado na tabela.
    """
    table = conn.load_table(table_name)
    row = table.find_one(name=name)
    if row is not None:
        return dict(row)
    else:
        raise exc.ItemNotStored(
            'Can\'t read "{}" because it\'s not stored in table "{}"'.format(name, table.table.name))


def select_all(conn, table_name):
    """Selecione todos os itens em uma tabela.

    A biblioteca do conjunto de dados retorna resultados como OrderedDicts.

    Parâmetros
    ----------
    table_name : str
    conn : dataset.persistence.database.Database

    Returns
    -------
    list
        lista de dicionários. Cada dict é um registro.
    """
    table = conn.load_table(table_name)
    rows = table.all()
    return list(map(lambda x: dict(x), rows))

######################################################
####################### UPDATE #######################
######################################################

def update_one(conn, name, price, quantity, table_name):
    """Atualize um único item na tabela.

     Observação: o método de atualização do conjunto de dados é um pouco contra-intuitivo de usar. Leia os documentos aqui: https://dataset.readthedocs.io/en/latest/quickstart.html#storing-data
     O conjunto de dados também possui uma funcionalidade de upsert: se existirem linhas com chaves correspondentes, elas serão atualizadas, caso contrário, uma nova linha será inserida na tabela.

     Parâmetros
    ----------
    name : str
    price : float
    quantity : int
    table_name : str
    conn : dataset.persistence.database.Database

    Raises
    ------
    exc.ItemNotStored: se o registro não estiver armazenado na tabela.
    """
    table = conn.load_table(table_name)
    row = table.find_one(name=name)
    if row is not None:
        item = {'name': name, 'price': price, 'quantity': quantity}
        table.update(item, keys=['name'])
    else:
        raise exc.ItemNotStored(
            'Can\'t update "{}" because it\'s not stored in table "{}"'.format(name, table.table.name))

######################################################
####################### DELETE #######################
######################################################

def delete_one(conn, item_name, table_name):
    """Excluir um único item em uma tabela.

     Parâmetros
    ----------
    item_name : str
    table_name : str
    conn : dataset.persistence.database.Database

    Raises
    ------
    exc.ItemNotStored: se o registro não estiver armazenado na tabela.
    """
    table = conn.load_table(table_name)
    row = table.find_one(name=item_name)
    if row is not None:
        table.delete(name=item_name)
    else:
        raise exc.ItemNotStored(
            'Can\'t delete "{}" because it\'s not stored in table "{}"'.format(item_name, table.table.name))

# def main():

#     conn = dataset.connect('sqlite:///:memory:')

#     table_name = 'items'
#     create_table(conn, table_name)

#     # CREATE
#     my_items = [
#         {'name': 'bread', 'price': 0.5, 'quantity': 20},
#         {'name': 'milk', 'price': 1.0, 'quantity': 10},
#         {'name': 'wine', 'price': 10.0, 'quantity': 5},
#     ]

#     insert_many(conn, items=my_items, table_name=table_name)
#     insert_one(conn, 'beer', price=2.0, quantity=5, table_name=table_name)
#     # se tentarmos inserir um objeto já armazenado, obtemos uma exceção ItemAlreadyStored
#      # insert_one(conn, 'cerveja', 2.0, 5, table_name=table_name)

#     # READ
#     print('SELECT milk')
#     print(select_one(conn, 'milk', table_name=table_name))
#     print('SELECT all')
#     print(select_all(conn, table_name=table_name))
#     # se tentarmos selecionar um objeto não armazenado, obtemos uma exceção ItemNotStored
#     # print(select_one(conn, 'pizza', table_name=table_name))

#     # UPDATE
#     print('UPDATE bread, SELECT bread')
#     update_one(conn, 'bread', price=1.5, quantity=5, table_name=table_name)
#     print(select_one(conn, 'bread', table_name=table_name))
#     # se tentarmos atualizar um objeto não armazenado, obtemos uma exceção ItemNotStored
#     # print('UPDATE pizza')
#     # update_one(conn, 'pizza', 9.5, 5, table_name=table_name)

#     # DELETE
#     print('DELETE beer, SELECT all')
#     delete_one(conn, 'beer', table_name=table_name)
#     print(select_all(conn, table_name=table_name))
#     # se tentarmos deletar um objeto não armazenado, obtemos uma exceção ItemNotStored
#     # print('DELETE peixe')
#     # delete_one(conn, 'peixe', table_name=table_name)


# if __name__ == '__main__':
#     main()