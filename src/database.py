from sqlalchemy import create_engine, Column, String, Integer, Date, inspect, UniqueConstraint, Float
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
from colorama import Fore
from sqlalchemy import Index
from sqlalchemy.exc import IntegrityError


Base = declarative_base()

# classe para definir a tabela no banco de dados
class OrdersTable(Base):
    # nome da tabela no banco de dados
    __tablename__ = 'pedidosfaturados'
    # evita que dados duplicados sejam inseridos no banco de dados
    #__table_args__ = (UniqueConstraint('pedido_faturamento', 'id_equipamento'),)

    # colunas da tabela
    id = Column(Integer, primary_key=True, autoincrement='auto')
    creat_at = Column(Date)
    codigo_cliente = Column(String, nullable=True)
    loja_cliente = Column(String, nullable=True)
    nome_do_cliente = Column(String, nullable=True)
    cnpj_do_cliente = Column(String, nullable=True)
    email = Column(String, nullable=True)
    contrato_legado = Column(String, nullable=True)
    projeto = Column(String, nullable=True)
    obra = Column(String, nullable=True)
    nome_da_obra = Column(String, nullable=True)
    numero_da_as = Column(String, nullable=True)
    pedido_de_remessa = Column(String, nullable=True)
    nota_de_remessa = Column(String, nullable=True)
    serie_da_nf_remessa = Column(String, nullable=True)
    data_de_remessa = Column(Date)
    cnpj_de_remessa = Column(String, nullable=True)
    id_equipamento = Column(String, nullable=True)
    id_equip_substituido = Column(String, nullable=True)
    data_da_substituicao = Column(Date)
    equipamento = Column(String, nullable=True)
    tipo_de_servico = Column(String, nullable=True)
    tipo_de_operacao = Column(String, nullable=True)
    produto = Column(String, nullable=True)
    descricao_do_produto = Column(String, nullable=True)
    quantidade = Column(Integer, nullable=True)
    valor_de_origem = Column(String, nullable=True)
    valor_unitario = Column(String, nullable=True)
    valor_bruto = Column(String, nullable=True)
    desconto = Column(String, nullable=True)
    acrescimo = Column(String, nullable=True)
    data_de_ativacao_legado = Column(Date)
    data_de_ativacao = Column(Date)
    ultimo_faturamento = Column(Date)
    data_proximo_faturamento = Column(Date)
    data_fim_locacao = Column(Date)
    dias_de_locacao = Column(Integer, nullable=True)
    prazo_do_contrato = Column(String, nullable=True)
    previsao_retirada = Column(Date, nullable=True)
    solicitacao_retirada = Column(Date, nullable=True)
    tipo_do_mes = Column(String, nullable=True)
    mes_fixo = Column(String, nullable=True)
    data_base_reajuste = Column(Date)
    indexador = Column(String, nullable=True)
    data_do_reajuste = Column(Date)
    indice_aplicado = Column(String, nullable=True)
    calculo_reajuste = Column(String, nullable=True)
    franquia = Column(String, nullable=True)
    class_faturamento = Column(String, nullable=True)
    cobra = Column('cobra?', String, nullable=True)
    data_entrada = Column(Date)
    centro_de_custo = Column(String, nullable=True)
    pedido_faturamento = Column(String, nullable=True)
    emissao_pedido = Column(Date)
    qtde_pedido = Column(Integer, nullable=True)
    vlr_unitario_pedido = Column(String, nullable=True)
    vlr_total_pedido = Column(String, nullable=True)
    percent_desconto = Column(String, nullable=True)
    vlr_desconto = Column(String, nullable=True)
    tes = Column(String, nullable=True)
    status_pedido = Column(String, nullable=True)
    natureza = Column(String, nullable=True)
    nf_de_faturamento = Column(String, nullable=True)
    serie_de_faturamento = Column(String, nullable=True)
    data_de_faturamento = Column(Date)
    cliente_faturamento = Column(String, nullable=True)
    loja_faturamento = Column(String, nullable=True)
    nome_cli_faturamento = Column(String, nullable=True)
    cnpj_de_faturamento = Column(String, nullable=True)
    qtde_faturamento = Column(String, nullable=True)
    vlr_unitario_faturamento = Column(String, nullable=True)
    vlr_total_faturamento = Column(String, nullable=True)
    periodo_de_faturamento = Column(String, nullable=True)
    origem_do_dado = Column(String, nullable=True)
    serie_do_equipamento = Column(String, nullable=True)

    
      

# classe para definir o nome da tabela no banco de dados    
class OrdersClass:
    def __init__(self, table_name):
        self.__table_name__ = table_name
        self.Table = OrdersTable


# classe para conexão com o banco de dados
class ConnectPostgresQL:
    # definindo construtor de classe com o parâmetros de conexão
    def __init__(self, host):
        self.engine = create_engine(host)
        
        self.Session = sessionmaker(bind=self.engine)
        

    # função para conexão com o banco de dados(PostgreSQL) self.engine.connect()
    def connect(self):
        return self.engine.connect()

    
    # função para criação do banco de dados e tabela
    def create_database(self):
        try:
            # inspeciona se a tabela existe no banco de dados
            inspector = inspect(self.engine)

            if not inspector.has_table(OrdersTable.__tablename__):
                Base.metadata.create_all(self.engine)
                print(f'Banco de Dados e Tabela {OrdersTable.__tablename__} criada com sucesso!')
            else:
                print(f'Dados já inseridos anteriormente na tabela {OrdersTable.__tablename__} !')
        except Exception as e:
            print(f'Erro ao criar banco de dados e tabel {OrdersTable.__tablename__}: {e}')


    # função para inserção de dados na tabela
    def insert_data(self, table_name, **kwargs):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                for key, value in kwargs.items():
                    if pd.isna(value) or isinstance(value, pd.Timestamp):
                        kwargs[key] = None
                    elif isinstance(value, str) and value == '-':
                        kwargs[key] = None
                    elif isinstance(value, str) and value == 'nan':
                        kwargs[key] = None
                    elif isinstance(value, str) and value == 'NaT':
                        kwargs[key] = None

                record = table(**kwargs)
                session.add(record)
                session.commit()
                print(Fore.GREEN + f'Dados {key} inseridos com sucesso!' + Fore.RESET)
            
        # caso o registro já exista no banco de dados, não insere novamente
        except IntegrityError as e:
            pass

        # caso ocorra algum erro, exibe o erro    
        except Exception as e:
            raise e

        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()

    
    # função para exclusão de dados da tabela
    def delete_data(self, table_name, id):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                session.query(table).filter(table.id == id).delete()
                session.commit()
                print(Fore.GREEN + f'Dados {id} excluídos com sucesso!' + Fore.RESET)

        # caso ocorra algum erro, exibe o erro
        except Exception as e:
            raise e

        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()
           

    # função para atualização de dados da tabela
    def update_data(self, table_name, id):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                session.query(table).filter(table.id == id).update()
                session.commit()
                print(Fore.GREEN + f'Dados {id} atualizados com sucesso!' + Fore.RESET)

        # caso ocorra algum erro, exibe o erro                
        except Exception as e:
            raise e
        
        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()


    # função para consulta de dados da tabela
    def query_data(self, table_name, query):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                result = session.query(table).filter(query).all()
                return result

        # caso ocorra algum erro, exibe o erro                
        except Exception as e:
            raise e
        
        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()

    
    # função para criar novo usuário
    def create_user(self, username, password):
        try:
            with self.engine.connect() as conn:
                conn.execute(f"CREATE USER {username} WITH PASSWORD '{password}'")
                print(Fore.GREEN + f'Usuário {username} criado com sucesso!' + Fore.RESET)
        except Exception as e:
            raise e 
        
    
    # função para conceder privilégios ao usuário
    def grant_privileges(self, username, database):
        try:
            with self.engine.connect() as conn:
                conn.execute(f"GRANT ALL PRIVILEGES ON DATABASE {database} TO {username}")
                print(Fore.GREEN + f'Privilégios concedidos ao usuário {username} com sucesso!' + Fore.RESET)
        except Exception as e:
            raise e


    def delete_all(self, table_name):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                session.query(table).delete()
                session.commit()
                print(Fore.GREEN + f'Todos os registros da tabela {table_name} excluídos com sucesso!' + Fore.RESET)

        # caso ocorra algum erro, exibe o erro
        except Exception as e:
            raise e

        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()


