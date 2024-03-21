from refactory import ExtractPipeline, TransformPipeline
from time import sleep


host_postgres = 'postgresql://postgres:123456789@localhost:5432/postgres'
extract_pipeline = ExtractPipeline(host_postgres)
transform_pipeline = TransformPipeline()

extractor_file_path = r"C:\Users\marcos.silvaext\Documents\01 - INPUT_DATA"
data_raw_path = r"C:\Users\marcos.silvaext\Documents\02 - DATA_RAW"
file_path_error = r"C:\Users\marcos.silvaext\Documents\05 - EXTRATORES COM ERROS"
column_name = 'Pedido Faturamento'



files_data = extract_pipeline.list_files_in_directory(extractor_file_path)

if files_data:
    extract_pipeline.verify_column(extractor_file_path, files_data, column_name, file_path_error)
    new_orders = extract_pipeline.identify_new_orders(files_data, column_name)

    if new_orders:
        extract_pipeline.create_files_with_new_orders(new_orders, data_raw_path, column_name)
        extract_pipeline.standard_columns_name(new_orders)
        extract_pipeline.add_new_columns_to_database(new_orders)    
        extract_pipeline.update_database(new_orders)
    

raw_files = transform_pipeline.list_files_in_raw_directory(data_raw_path)

if raw_files:
        
        transform_pipeline.format_columns_values(raw_files)
        transform_pipeline.format_columns_cnpj(raw_files)
        # transform_pipeline.format_columns_date(raw_files)
        transform_pipeline.save_to_excel(raw_files, data_raw_path)