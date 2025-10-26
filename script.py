import requests
import xml.etree.ElementTree as ET
import re
import time
import os
import csv

def salvar_informacoes_produto(info_produto):
    # Nome do arquivo com data
    nome_arquivo = f"produtos_{time.strftime('%Y-%m-%d')}.xlsx"

    # Lista completa de campos
    campos = [
        'Origem', 'Link', 'EAN', 'Nome', 'Descrição', 'Categoria do Produto',
        'Link Imagem', 'Path Imagem Principal', 'Princípio Ativo', 'Laboratório',
        'Forma Farmacéutica', 'Dose', 'Quantidade Embalagem', 'Tags', 'Link Bula',
        'Path Bula', 'Link Image Laboratório', 'Path Imagem Laboratório', 'Marca do Produto',
        'Tipo do Medicamento', 'Necessidade de Receita', 'Tarja do Medicamento',
        'Forma de conservação', 'Modo de Uso', 'Registro no Ministério da Saúde (M.S)',
        'Fabricante', 'CNPJ do Fabricante:', 'Informações Marca', 'Indicações',
        'Funcionamento', 'Contraindicação', 'Advertências e Iterações', 'Armazenamento',
        'Modo de uso/Como Usar', 'Em caso de Esquecimento', 'Reações Adversas', 'Superdose'
    ]

    # Verifica se o arquivo já existe
    arquivo_existe = os.path.isfile(nome_arquivo)

    # Abre o arquivo CSV no modo append
    with open(nome_arquivo, mode='a', newline='', encoding='utf-8') as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=campos, delimiter=';')

        # Se o arquivo acabou de ser criado, escreve o cabeçalho
        if not arquivo_existe:
            escritor.writeheader()

        # Garante que o dicionário tenha todas as chaves (mesmo que vazias)
        for campo in campos:
            if campo not in info_produto:
                info_produto[campo] = ''

        # Escreve a linha
        escritor.writerow(info_produto)

    print(f"✅ Produto {info_produto['Nome']} salvo em {nome_arquivo} com sucesso!")


def mapear_campos(info_produto):
    dados = {}
    info_produto = info_produto[0]
    dados['Origem'] = 'São João Farmácias'
    dados['Link'] = info_produto['link']
    dados['EAN'] = info_produto.get('EAN', [''])[0]
    dados['Nome'] = info_produto.get('productName', '')
    dados['Descrição'] = info_produto.get('metaTagDescription', '').replace('<br/>', ' ').replace('\n', ' ').strip()
    dados['Categoria do Produto'] = ''.join([cat for cat in info_produto.get('categories', [])])
    dados['Link Imagem'] = ' > '.join([     img['imageUrl']     for item in info_produto['items']      if item.get('images')      for img in item['images']      if 'imageUrl' in img ])
    dados['Path Imagem Principal'] = ' > '.join([     img['imageUrl']     for item in info_produto['items']      if item.get('images')      for img in item['images']      if 'imageUrl' in img ])
    dados['Princípio Ativo'] = ' >  '.join(info_produto.get('Princípio Ativo', []))
    dados['Laboratório'] = info_produto.get('brand', '')
    dados['Forma Farmacéutica'] = info_produto.get('pharmaceuticalForm', '')
    dados['Dose'] = ' > '.join(info_produto.get('Dosagem', []))
    dados['Quantidade Embalagem'] = info_produto['items'][0]['unitMultiplier']
    dados['Tags'] = ', '.join(info_produto.get('tags', []))
    dados['Link Bula'] = info_produto.get('leafletUrl', '')
    dados['Path Bula'] = dados['Link Bula'].split('/')[-1] if dados['Link Bula'] else ''
    dados['Marca do Produto'] = info_produto.get('brand', '')


    dados['Link Image Laboratório'] = ''
    dados['Path Imagem Laboratório'] = ''
    dados['Tipo do Medicamento'] = ''
    dados['Necessidade de Receita'] = info_produto['skuControlado'][0]
    dados['Tarja do Medicamento'] = ''
    dados['Forma de conservação'] = ''
    dados['Modo de Uso'] = ''
    dados['Registro no Ministério da Saúde (M.S)'] = ''
    dados['Fabricante'] = ''
    dados['CNPJ do Fabricante:'] = ''
    dados['Informações Marca'] = ''
    dados['Indicações'] = ''
    dados['Funcionamento'] = ''
    dados['Contraindicação'] = ''
    dados['Advertências e Iterações'] = ''
    dados['Armazenamento'] = ''
    dados['Modo de uso/Como Usar'] = ''
    dados['Em caso de Esquecimento'] = ''
    dados['Reações Adversas'] = ''
    dados['Superdose'] = ''

    # Campos adicionais podem ser mapeados aqui conforme necessário

    return dados

def webscraping():
    sitemap = 'https://www.saojoaofarmacias.com.br/sitemap/product-0.xml'
    resposta = requests.get(sitemap)

    if resposta.status_code == 200:
        try:
            root = ET.fromstring(resposta.text)

            urls = [loc.text for loc in root.iter() if 'loc' in loc.tag]

            if urls:
                for url in urls:
                    pagina = requests.get(url).text
                    padrao_id = re.search(r'productId["\']?\s*[:=]\s*["\']?(\d+)', pagina)

                    if padrao_id:
                        product_id = padrao_id.group(1)
                        url_request = f'https://www.saojoaofarmacias.com.br/api/catalog_system/pub/products/search/?fq=productId:{product_id}'
                        info_produto = requests.get(url_request).json()
                        campos = mapear_campos(info_produto)
                        salvar_informacoes_produto(campos)
                    time.sleep(0.5)
        except ET.ParseError:
            print("O conteúdo retornado não é um XML válido.")
    else:
        print(f"Erro ao acessar o sitemap: {resposta.status_code}")

if __name__ == '__main__':
    webscraping()
