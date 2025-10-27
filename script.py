import requests
import xml.etree.ElementTree as ET
import re
import time
import os
import csv
from urllib.parse import urlparse
from pathlib import Path
import hashlib

NM_ARQUIVO = f"produtos_{time.strftime('%Y-%m-%d')}.csv"
DIRETORIO_IMAGENS = "imagens_produtos"
# URL_SITEMAP = 'https://www.saojoaofarmacias.com.br/sitemap/product-10.xml'
URL_SITEMAP = 'https://www.saojoaofarmacias.com.br/sitemap.xml'

def baixar_imagem(url_imagem, ean_produto, idx_imagem=0):
    """
    Baixa uma imagem da URL fornecida e salva no diretório de imagens
    """
    try:
        if not url_imagem:
            return ""
        
        # Verifica se é placeholder (não baixa)
        if 'vtexassets.com' in url_imagem:
            print(f"  ⏭️ Pulando placeholder: {url_imagem}")
            return ""
        
        # Cria o diretório se não existir
        Path(DIRETORIO_IMAGENS).mkdir(exist_ok=True)
        
        # Faz o download da imagem
        response = requests.get(url_imagem, timeout=30)
        response.raise_for_status()
        
        # Gera nome baseado no EAN
        parsed_url = urlparse(url_imagem)
        extensao = os.path.splitext(parsed_url.path)[1] or '.jpg'
        
        # Nome do arquivo: ean_01, ean_02, etc.
        nome_arquivo = f"{ean_produto}_{idx_imagem+1:02d}{extensao}"
        
        # Remove caracteres inválidos do nome do arquivo
        nome_arquivo = re.sub(r'[<>:"/\\|?*]', '_', nome_arquivo)
        
        caminho_arquivo = os.path.join(DIRETORIO_IMAGENS, nome_arquivo)
        
        # Salva a imagem
        with open(caminho_arquivo, 'wb') as f:
            f.write(response.content)
        
        print(f"  📸 Imagem baixada: {nome_arquivo}")
        return caminho_arquivo
        
    except Exception as e:
        print(f"  ❌ Erro ao baixar imagem {url_imagem}: {str(e)}")
        return ""

def salvar_informacoes_produto(idx, total, info_produto):
    # Nome do arquivo com data

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

    # Baixa as imagens do produto
    urls_imagens = info_produto.pop('_urls_imagens', [])
    caminhos_imagens = []
    
    if urls_imagens:
        print(f"  📥 Baixando {len(urls_imagens)} imagem(ns) para {info_produto['Nome']} (EAN: {info_produto['EAN']})...")
        for i, url_imagem in enumerate(urls_imagens):
            caminho_imagem = baixar_imagem(url_imagem, info_produto['EAN'], i)
            if caminho_imagem:
                caminhos_imagens.append(caminho_imagem)
        
        # Atualiza os campos de imagem com os caminhos locais
        info_produto['Path Imagem Principal'] = ' > '.join(caminhos_imagens)

    # Verifica se o arquivo já existe
    arquivo_existe = os.path.isfile(NM_ARQUIVO)

    # Abre o arquivo CSV no modo append
    with open(NM_ARQUIVO, mode='a', newline='', encoding='utf-8') as arquivo:
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

    print(f"{idx} de {total} - ✅ Produto {info_produto['Nome']} salvo em {NM_ARQUIVO} com sucesso!")


def mapear_campos(info_produto):
    dados = {}
    info_produto = info_produto[0]
    dados['Origem'] = 'São João Farmácias'
    dados['Link'] = info_produto['link']
    dados['EAN'] = info_produto['items'][0]['ean']
    dados['Nome'] = info_produto.get('productName', '')
    try:
        dados['Descrição'] = info_produto.get('description', '').replace('<br/>', ' ').replace('\n', ' ').strip()
    except Exception as error:
        print(error)
    dados['Categoria do Produto'] = ''.join([cat for cat in info_produto.get('categories', [])])
    
    # Extrai URLs das imagens de forma mais robusta
    urls_imagens = []
    for item in info_produto.get('items', []):
        if item.get('images'):
            for img in item['images']:
                if 'imageUrl' in img and img['imageUrl']:
                    urls_imagens.append(img['imageUrl'])
    
    dados['Link Imagem'] = ' > '.join(urls_imagens)
    dados['Path Imagem Principal'] = ' > '.join(urls_imagens)
    
    # Armazena as URLs para download posterior
    dados['_urls_imagens'] = urls_imagens
    
    dados['Princípio Ativo'] = ' >  '.join(info_produto.get('Princípio Ativo', []))
    dados['Laboratório'] = info_produto.get('brand', '')
    dados['Forma Farmacéutica'] = info_produto.get('pharmaceuticalForm', '')
    dados['Dose'] = ' > '.join(info_produto.get('Dosagem', []))
    dados['Quantidade Embalagem'] = info_produto.get('Quantidade')[0] if info_produto.get('Quantidade') else info_produto['items'][0]['unitMultiplier']
    dados['Tags'] = ', '.join(info_produto.get('tags', []))
    dados['Link Bula'] = info_produto.get('leafletUrl', '')
    dados['Path Bula'] = dados['Link Bula'].split('/')[-1] if dados['Link Bula'] else ''
    dados['Marca do Produto'] = info_produto.get('brand', '')


    dados['Link Image Laboratório'] = ''
    dados['Path Imagem Laboratório'] = ''
    dados['Tipo do Medicamento'] = ''
    dados['Necessidade de Receita'] = ', '.join(info_produto.get('skuControlado', []))
    dados['Tarja do Medicamento'] = ''
    dados['Forma de conservação'] = ''
    dados['Modo de Uso'] = ', '.join(info_produto.get('Modo de Uso', []))
    dados['Registro no Ministério da Saúde (M.S)'] = ''
    dados['Fabricante'] = info_produto.get('Fabricante')[0] if info_produto.get('Fabricante') else ''
    dados['CNPJ do Fabricante:'] = ''
    dados['Informações Marca'] = ''
    dados['Indicações'] = ', '.join(info_produto.get('Indicações de Uso', []))
    dados['Funcionamento'] = ''
    dados['Contraindicação'] =  ', '.join(info_produto.get('Contraindicações', []))
    dados['Advertências e Iterações'] = ', '.join(info_produto.get('Precauções', []))
    dados['Armazenamento'] = ''
    dados['Modo de uso/Como Usar'] = ''
    dados['Em caso de Esquecimento'] = ''
    dados['Reações Adversas'] = ', '.join(info_produto.get('Precauções', []))
    dados['Superdose'] = ''

    return dados

def webscraping():
    resposta = requests.get(URL_SITEMAP)
    root = ET.fromstring(resposta.text)

    urls_produtos = []
    for sitemap in root.findall(".//{*}sitemap"):
        loc = sitemap.find("{*}loc")
        if loc is not None and "product-" in loc.text:
            urls_produtos.append(loc.text.strip())

    for url in urls_produtos:
        resposta = requests.get(url)
        if resposta.status_code == 200:
            try:
                root = ET.fromstring(resposta.text)

                urls = [loc.text for loc in root.iter() if 'loc' in loc.tag]

                if urls:
                    for idx, url in enumerate(urls, start=1):
                        pagina = requests.get(url).text
                        padrao_id = re.search(r'productId["\']?\s*[:=]\s*["\']?(\d+)', pagina)

                        if padrao_id:
                            product_id = padrao_id.group(1)
                            url_request = f'https://www.saojoaofarmacias.com.br/api/catalog_system/pub/products/search/?fq=productId:{product_id}'
                            info_produto = requests.get(url_request).json()
                            campos = mapear_campos(info_produto)
                            salvar_informacoes_produto(idx, len(urls), campos)
                        time.sleep(0.5)
            except ET.ParseError:
                print("O conteúdo retornado não é um XML válido.")
        else:
            print(f"Erro ao acessar o sitemap: {resposta.status_code}")

if __name__ == '__main__':
    webscraping()
