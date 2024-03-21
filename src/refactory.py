import pandas as pd
import os
import shutil
import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment



import pandas as pd
import os
from sqlalchemy import create_engine
import shutil
from colorama import Fore
from database import ConnectPostgresQL, OrdersTable
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment


class ExtractPipeline:
    def __init__(self, host):
        self.db_connection = ConnectPostgresQL(host)
        self.session = self.db_connection.Session()


    def list_files_in_directory(self, extractor_file_path):
        files_data = []
        try:
            for filename in os.listdir(extractor_file_path):
                if filename.endswith('.xlsx') and not filename.startswith('~$'):
                    file_path = os.path.join(extractor_file_path, filename)
                    extract_file = pd.read_excel(file_path, sheet_name='2-Resultado', engine='openpyxl', header=1)
                    files_data.append((filename, extract_file))
                else:
                    print('Arquivo não suportado...')
                    continue                
            return files_data
        
        except FileNotFoundError:
            print('Diretório não encontrado...')
        except PermissionError:
            print('Sem permissão para acessar o diretório...')
        except Exception as e:
            print(f'Erro inesperado: {e}')

        
    def verify_column(self, extractor_file_path, df_list, column_name, file_path_error):

        try:
            for filename, df in df_list:
                if column_name in df.columns:
                    return True

                else:
                    print(f'Coluna Pedido Faturamento não encontrada no arquivo {filename}...')
                    # move o arquivo para a pasta de erro
                    new_filename = f"{os.path.splitext(filename)[0]}_falta_coluna_pedido.xlsx"
                    # pega o caminho completo do arquivo com erro
                    old_file_path = os.path.join(extractor_file_path, filename)
                    # pega o caminho completo da pasta de erro
                    error_path = os.path.join(file_path_error, new_filename)
                    # cria a pasta de erro se não existir            
                    os.makedirs(file_path_error, exist_ok=True)                    
                    # move o arquivo para a pasta de erro
                    shutil.move(old_file_path, error_path)
                    print(f'Arquivo {filename} movido para a pasta de arquivos com erros...')
                    
        except FileNotFoundError:
            print('Arquivo não encontrado...')
        except PermissionError:
            print('Sem permissão para acessar o arquivo...')
        except Exception as e:
            print(f'Erro inesperado em verificar colunas: {e}')
       
    
    def identify_new_orders(self, df_list, column_name):
        new_orders_list = []
        try:
            for filename, df in df_list:
                if column_name in df.columns:
                    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
                    print(f'Total de registros no extrator {filename}: {len(df)}')

                    # Filtra apenas pedidos recentes
                    recent_orders = df[df[column_name] > 0]
                    # Remove valores nulos
                    recent_orders = recent_orders[recent_orders[column_name].notna()]
                    # Define novamente o tipo da coluna como inteiro
                    recent_orders[column_name] = recent_orders[column_name].astype(int)

                    # Consulta o banco de dados com os pedidos já inseridos anteriormente
                    existing_orders = set(int(order) for order in pd.read_sql_query(
                        f'SELECT DISTINCT pedido_faturamento FROM {OrdersTable.__tablename__}',
                        self.db_connection.engine)['pedido_faturamento'])

                    # Compara os pedidos do extrator e identifica os novos
                    new_orders_file = set(recent_orders[column_name]) - existing_orders
                    print(f'Novos pedidos encontrados no arquivo {filename}: {len(new_orders_file)}')

                    # Cria um DataFrame apenas com os novos pedidos
                    new_orders_df = recent_orders[recent_orders[column_name].isin(new_orders_file)]

                    # Adiciona o novo DataFrame à lista de novos pedidos, se não estiver vazio
                    if not new_orders_df.empty:
                        new_orders_list.append(new_orders_df)
                else:
                    print(f'Coluna {column_name} não encontrada no arquivo {filename}...')

            return new_orders_list

        except FileNotFoundError:
            print(Fore.RED + f'Arquivo não encontrado' + Fore.RESET)
        except PermissionError:
            print(Fore.RED + f'Sem permissão para acessar o arquivo' + Fore.RESET)
        except Exception as e:
            print(Fore.RED + f'Erro inesperado: {e}' + Fore.RESET)
                
        
    def columns_keeper(self):
        columns = ['CODIGO CLIENTE', 'NOME DO CLIENTE', 'LOJA CLIENTE', 'CNPJ DO CLIENTE', 'CNPJ DE FATURAMENTO',
                    'PROJETO', 'OBRA', 'ID EQUIPAMENTO', 'EQUIPAMENTO', 'DESCRICAO DO PRODUTO', 'DATA DE ATIVACAO LEGADO', 
                    'PERIODO DE FATURAMENTO', 'DIAS DE LOCACAO', 'VALOR UNITARIO', 'VALOR BRUTO', 'DATA DE ATIVACAO', 'QUANTIDADE', 
                    'VLR. TOTAL PEDIDO', 'VLR. TOTAL FATURAMENTO',
                    'NF DE FATURAMENTO',  'DATA DE FATURAMENTO', 'DATA BASE REAJUSTE', 'VALOR DE ORIGEM', 
                    'INDEXADOR', 'CALCULO REAJUSTE', 'INDICE APLICADO', 'ACRESCIMO', 'CONTRATO LEGADO', 'PEDIDO FATURAMENTO'
                    ]
        
        
        return columns
    
    
    def rename_columns(self):
        columns = {'VLR. TOTAL PEDIDO': 'VALOR TOTAL GERADO', 'VLR. TOTAL FATURAMENTO': 'VALOR TOTAL FATURAMENTO'}
        return columns

    
    def create_files_with_new_orders(self, df_list, data_raw_path, column_name):
        try:
            if df_list:
                for df in df_list:
                    if not df.empty:  # Verifica se o DataFrame não está vazio
                        os.makedirs(data_raw_path, exist_ok=True)

                        if 'Nome do Cliente' in df.columns:
                            for order_number, order_group in df.groupby(column_name):
                                client_name_valid = order_group['Nome do Cliente'].iloc[0]\
                                    .translate(str.maketrans('.', ' ', r'\/:*?"<>|'))
                                filename = f'{order_number}_{client_name_valid}.xlsx'
                                file_path = os.path.join(data_raw_path, filename)
                                # salva o arquivo com cabeçalho em maiúsculas
                                order_group.columns = map(str.upper, order_group.columns)
                                # seleciona apenas as colunas necessárias
                                order_group = order_group[self.columns_keeper()]
                                # renomeia as colunas
                                order_group.rename(columns=self.rename_columns(), inplace=True)

                                # salva o arquivo
                                order_group.to_excel(file_path, sheet_name='2-Resultado', index=False)
                                                            
                        else:
                            print('Coluna Nome do Cliente não encontrada...')
            else:
                print('Nenhum novo pedido encontrado...')
                return False
        except FileNotFoundError:
            print('Arquivo não encontrado...')
        except PermissionError:
            print('Sem permissão para acessar o arquivo...')
        except Exception as e:
            print(f'Erro inesperado: {e}')

    
    def standard_columns_name(self, df_list):
        try:
            
            for df in df_list:
                if not df.empty:
                    df.columns = df.columns.str.lower().str.replace(' ', '_')\
                        .str.replace('.', '').str.replace('-', '').str.replace('ç', 'c').str.replace('?', '')                    
            print(f'Arquivos padronizados com sucesso...')

        except FileNotFoundError:
            print('Diretório não encontrado...')
        except PermissionError:
            print('Sem permissão para acessar o diretório...')
        except Exception as e:
            print(f'Erro inesperado em padronizar colunas: {e}')

                    
    def update_database(self, df_list):
        try:
            # Inicia a conexão com o banco de dados
            engine = self.db_connection.engine
            conn = engine.connect()
                    
            for df in df_list:
                if not df.empty:
                    df.to_sql(OrdersTable.__tablename__, conn, if_exists='append', index=False, chunksize=1000)
            
            print(f'Novos pedidos salvos no banco de dados.')
        except Exception as e:
            print(e)


    def add_new_columns_to_database(self, df_list):
        try:
            if df_list:
                all_columns = set()
                for df in df_list:
                    all_columns.update(df.columns)

                table_columns = set(OrdersTable.__table__.columns.keys())

                for column_name in all_columns:
                    if column_name not in table_columns:
                        try:
                            self.db_connection.engine.execute(
                                f'ALTER TABLE {OrdersTable.__tablename__} ADD COLUMN "{column_name}" TEXT'
                            )
                            print(Fore.CYAN + f'Coluna {column_name} adicionada com sucesso...' + Fore.RESET)
                        except Exception as e:
                            continue       
                    
            else:
                print('Nenhum arquivo encontrado...')
        except FileNotFoundError:
            print('Arquivo não encontrado...')
        except PermissionError:
            print('Sem permissão para acessar o arquivo...')
        except Exception as e:
            print(f'Erro inesperado: {e}')




class TransformPipeline:

    
    def list_files_in_raw_directory(self, extractor_file_path):
        files_data = []
        try:
            for filename in os.listdir(extractor_file_path):
                if filename.endswith('.xlsx') and not filename.startswith('~$'):
                    file_path = os.path.join(extractor_file_path, filename)
                    extract_file = pd.read_excel(file_path, sheet_name='2-Resultado', engine='openpyxl')
                    files_data.append((filename, extract_file))
                else:
                    print('Arquivo não suportado...')
                    continue                
            return files_data
        
        except FileNotFoundError:
            print('Diretório não encontrado...')
        except PermissionError:
            print('Sem permissão para acessar o diretório...')
        except Exception as e:
            print(f'Erro inesperado: {e}')


    def format_columns_values(self, df_list, columns_to_format=['VALOR TOTAL GERADO', 'VALOR TOTAL FATURAMENTO']):
        """Função para tratar os dados das colunas especificadas,
        passando de strings para floats no formato que o Python
        entende como números para soma, evitando cálculo incorreto 
        do arquivo consolidado."""
        try:
            for filename, df in df_list:
                if isinstance(df, pd.DataFrame):
                    for col in columns_to_format:
                        if col in df.columns:
                            df[col] = df[col].astype(str).str.replace('.', '').str.replace(',', '.')
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        else:
                            print(f'Coluna {col} não encontrada no arquivo {filename}...')
                    print(f'Colunas formatadas com sucesso no arquivo {filename}...')
                else:
                    print(f"Erro: O objeto associado ao arquivo {filename} não é um DataFrame.")
        except Exception as e:
            print(f'Erro ao formatar colunas: {e}')
                    

    def format_columns_cnpj(self, df_list, columns_to_format=['CNPJ DO CLIENTE', 'CNPJ DE FATURAMENTO']):
        try:
            for filename, df in df_list:
                if isinstance(df, pd.DataFrame):
                    for col in columns_to_format:
                        if col in df.columns:
                            df[col] = df[col].astype(str).str.replace(r'(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})', r'\1.\2.\3/\4-\5')
                            print(f'Colunas de CNPJ formatadas com sucesso no arquivo {filename}...')
                else:
                    print(f"Erro: O objeto associado ao arquivo {filename} não é um DataFrame.")
        except Exception as e:
            print(f'Erro ao formatar CNPJ: {e}')


    def format_columns_date(self, df_list, columns_to_format=['DATA DE ATIVACAO', 'DATA DE FATURAMENTO', 'DATA BASE REAJUSTE']):
        try:
            for filename, df in df_list:
                if isinstance(df, pd.DataFrame):
                    for col in columns_to_format:
                        if col in df.columns:
                            pd.api.types.is_datetime64_any_dtype(df[col])
                            df[col] = df[col].dt.strftime('%d/%m/%Y')  
                            print(f'Coluna de data {col} formatada com sucesso no arquivo {filename}...')
                        else:
                            df[col] = pd.to_datetime(df[col].loc[df[col].notna()], format='%d/%m/%Y', errors='coerce')
                            print(f'Coluna {col} não encontrada no arquivo {filename}.')
                else:
                    print(f"Erro: O objeto associado ao arquivo {filename} não é um DataFrame.")
        except Exception as e:
            print(f'Erro ao formatar data: {e}')
            
       
    def save_to_excel(self, df_list, file_path):
        try:
            for filename, df in df_list:
                if isinstance(df, pd.DataFrame):
                    path = os.path.join(file_path, filename)
                    df.to_excel(path, sheet_name='Relatório', index=False, engine='openpyxl')
                    print(f'Arquivo {filename} salvo com sucesso...')
                else:
                    print(f"Erro: O objeto associado ao arquivo {filename} não é um DataFrame.")
        except Exception as e:
            print(f'Erro ao salvar arquivo: {e}')
    
    
    def format_styles_report_sheet(self, df_list, directory):
        try:
            for filename, df in df_list:
                if isinstance(df, pd.DataFrame):
                    # Aplicar a lógica de conversão na coluna 'VLR TOTAL FATURAMENTO' e 'VALOR TOTAL GERADO'
                    df['VLR TOTAL FATURAMENTO'] = df['VLR TOTAL FATURAMENTO'].apply(self.corrigir_valor_faturamento)
                    df['VALOR TOTAL GERADO'] = df['VALOR TOTAL GERADO'].apply(self.corrigir_valor_faturamento)

                    # Converte as colunas para números
                    numeric_columns = ['VLR TOTAL FATURAMENTO', 'VALOR TOTAL GERADO']
                    for col in numeric_columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                    # Caminho completo do arquivo
                    file_path = os.path.join(directory, filename)
                    
                    # Carrega o arquivo Excel existente
                    wb = load_workbook(file_path)
                    ws = wb.active

                    # Formatação das células
                    for column in range(1, ws.max_column + 1):
                        col_letter = ws.cell(row=1, column=column).column_letter
                        ws.column_dimensions[col_letter].width = 25
                        cell = ws.cell(row=1, column=column)
                        cell.font = Font(color='FFFFFF', bold=True, name='Lato Regular', size=10)
                        cell.fill = PatternFill(start_color='FF0000', end_color='FF0000', fill_type='solid')
                        cell.alignment = Alignment(horizontal='center', vertical='center')
                        ws.row_dimensions[1].height = 24

                    # Salva as alterações no arquivo
                    wb.save(file_path)

                    print(f'Planilha {filename} formatada com sucesso.')
                else:
                    print(f"Erro: O objeto associado ao arquivo {filename} não é um DataFrame.")
        except Exception as e:
            print(f'Erro inesperado: {e}')


    def corrigir_valor_faturamento(self, valor):
        try:
            if isinstance(valor, str):
                valor = valor.replace('R$', '').replace('.', '').replace(',', '.')
                return valor
            else:
                return valor
        except Exception as e:
            print(f'Erro inesperado: {e}')
    
    
    def generate_synthesis_sheet(self, df_list, directory):
        try:
            for filename, df in df_list:
                if isinstance(df, pd.DataFrame):
                    # Criar a planilha 'Síntese'
                    sintese_df = df.groupby(['PROJETO', 'OBRA', 'CONTRATO LEGADO'], as_index=False).agg({'VALOR TOTAL GERADO': 'sum', 'VLR TOTAL FATURAMENTO': 'sum'})
                    sintese_df = sintese_df.rename(columns={'VLR TOTAL FATURAMENTO': 'VALOR TOTAL FATURADO'})
                    
                    # Caminho completo do arquivo
                    file_path = os.path.join(directory, filename)
                    
                    # Carrega o arquivo Excel existente
                    wb = load_workbook(file_path)
                    
                    # Adiciona a planilha 'Síntese'
                    wb.create_sheet('Síntese')
                    ws = wb['Síntese']
                    
                    # Escreve os cabeçalhos
                    headers = sintese_df.columns
                    for col_idx, header in enumerate(headers, start=1):
                        ws.cell(row=1, column=col_idx, value=header)
                        ws.cell(row=1, column=col_idx).font = Font(bold=True)
                    
                    # Escreve os dados da 'Síntese' na planilha
                    for r_idx, row in sintese_df.iterrows():
                        for c_idx, value in enumerate(row, start=1):
                            ws.cell(row=r_idx + 2, column=c_idx, value=value)
                    
                    # Salva as alterações no arquivo
                    wb.save(file_path)

                    print(f'Planilha "Síntese" criada com sucesso em {filename}.')
                else:
                    print(f"Erro: O objeto associado ao arquivo {filename} não é um DataFrame.")
        except Exception as e:
            print(f'Erro inesperado: {e}')



class LoadPipeline:

    def move_files_to_month_subfolder(self, data_raw_path, target_directory):
            try:
                files_to_move = [file for file in os.listdir(data_raw_path)\
                                if file.endswith('.xlsx') \
                                and not file.startswith('~$')]
                
                # Cria a subpasta do mês atual
                current_date = datetime.now()
                # formata a data atual para o formato mm-aaaa
                month_year = current_date.strftime('%m-%Y')

                for file_to_move in files_to_move:
                    current_file_path = os.path.join(data_raw_path, file_to_move)

                    # extrai o nome do arquivo sem a extensão
                    client_name_start = file_to_move.find('_') + 1
                    client_name_end = file_to_move.find('.', client_name_start)
                    client_name = file_to_move[client_name_start:client_name_end]

                    # encontra o caminho completo do arquivo no destino
                    current_file_path_with_month = os.path.join(target_directory, client_name, month_year)

                    # cria o diretório para o arquivo movido
                    if not os.path.exists(current_file_path_with_month):
                        os.makedirs(current_file_path_with_month)
                        print(f'Pasta {current_file_path_with_month} criada com sucesso...')

                    # identifica o caminho completo do arquivo de destino
                    destination_file_path = os.path.join(current_file_path_with_month, file_to_move)

                    # verifica se o arquivo já existe no diretório de destino
                    if os.path.exists(destination_file_path):
                        print(f'Arquio {file_to_move} já existe no diretório {current_file_path_with_month}')
                        os.remove(destination_file_path)

                    try:
                        shutil.move(current_file_path, current_file_path_with_month)
                        print(f'Arquivo {file_to_move} movido para {current_file_path_with_month} com sucesso')

                    except PermissionError as e:
                        print(f'Arquvio {file_to_move} está aberto: {e}')
                        return False
                    except OSError as e:
                        print(f'Erro ao mover o arquivo {file_to_move} possívelmente aberto: {e}')
                        return False
            except Exception as e:
                print(f'Erro inesperado: {e}')
                return False