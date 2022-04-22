# CRUD Python

## Requisitos:
- Python versão >= 3.9.12
- Biblioteca Dataset
    O conjunto de dados fornece uma camada de abstração simples que remove a maioria das instruções SQL diretas sem a necessidade de um modelo ORM completo
- Biblioteca SqlAlchemy 
    Ele fornece um conjunto completo de padrões de persistência de nível empresarial bem conhecidos, projetados para acesso eficiente e de alto desempenho ao banco de dados, adaptados em uma linguagem de domínio simples e Python.
    Neste projeto ele é utilizado para tratar exceções.

## Explicação adicional:
A arquitetura do código segue o formato MVC. As camadas Model e Controller buscam funções pré definidas nas respectivas bibliotecas.