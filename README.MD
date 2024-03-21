# Relatório de Faturamento Automatizado
O objetivo deste projeto é automatizar a criação do relatório final de faturamento a ser enviado mensalmente ao cliente. O departamento de faturamento enfrenta desafios na elaboração do relatório padrão quando o ambiente Totvs está em produção. Atualmente, o relatório emitido pelo Totvs não contém todas as informações essenciais para o cliente.

# Funcionalidades e Objetivos
A automação resolverá esse problema ao identificar novos dados de pedidos gerados no ambiente Totvs, seja manualmente pelo analista ou automaticamente pelo próprio ERP. Utilizando um conjunto de funções Python organizadas em uma pipeline, a automação processará esses dados e gerará um relatório no formato padrão. Esse relatório permitirá que o cliente valide as informações e efetue o faturamento de maneira mais eficiente.

# Benefícios
Os benefícios da automação no projeto incluem:

- **Eficiência Operacional:**
  - Ganho significativo de eficiência para o departamento de faturamento.
  - Eliminação da necessidade de intervenção manual, reduzindo substancialmente o tempo necessário para gerar e enviar o documento ao cliente mensalmente.

- **Mitigação de Erros Operacionais:**
  - Redução da probabilidade de omissões ou erros relacionados à coleta manual de dados.
  - Processo consistente e livre de falhas, garantindo a precisão das informações no relatório final.

# Etapas da Pipeline
No que diz respeito às etapas específicas da pipeline:

1. **Identificação de Novos Pedidos:**
   - Essa fase é crucial e pode ser realizada tanto manualmente pelo analista quanto automaticamente pelo ERP.

2. **Integração de Informações do Cliente:**
   - Os novos pedidos identificados são integrados às demais informações necessárias do cliente.

3. **Busca e Agregação de Dados:**
   - Envolve a busca e agregação de dados relevantes de diversas fontes.

4. **Geração do Relatório Final:**
   - Assegura que todas as informações essenciais estejam incluídas no relatório final de faturamento.

4. **Consolidação de Relatório:**
   - Consolidar todos os pedidos em um único arquivo para envio ao cliente pré-validar o faturamento.
## Melhorias Adicionais

Para aprimorar ainda mais o raciocínio, seria útil considerar:

- **Implementação de Medidas de Segurança e Controle de Qualidade:**
  - Garantir a integridade dos dados e a confiabilidade do relatório gerado.
  - Fortalecer a confiança do cliente no sistema automatizado.

Este projeto visa não apenas otimizar o processo de faturamento, mas também garantir a qualidade e confiabilidade das informações fornecidas ao cliente.

### Como Executar o Projeto

1. Ative o ambiente virtual na raiz do projeto:   
   ```bash
   .\venv\Scripts\activate
   ```   
2. Instale as dependências
```bash
pip install -r requirements.txt
```
3. Navegue até a pasta 'scripts'
```bash
cd final_report\scripts
```
Execute o arquivo loopmain.py
```bash
python loopmain.py
```

### Créditos 
Desenvolvedor Python -> Marcos Vinicius Madeira
Key-User -> Eliane Celeste Borges Vilela
Área de negócios Arklok -> Faturamento
Ano -> Nov/2023 



