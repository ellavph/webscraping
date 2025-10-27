import requests
import xml.etree.ElementTree as ET
import re
import time
import os
import csv
from urllib.parse import urlparse
from pathlib import Path
import hashlib
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

NM_ARQUIVO = f"produtos_{time.strftime('%Y-%m-%d')}.csv"
DIRETORIO_IMAGENS = "imagens_produtos"
# URL_SITEMAP = 'https://www.saojoaofarmacias.com.br/sitemap/product-10.xml'
URL_SITEMAP = 'https://www.saojoaofarmacias.com.br/sitemap.xml'

# Cache para EANs já processados (otimização de velocidade)
EANS_PROCESSADOS = set()

# Configurações de concorrência
MAX_CONCURRENT_REQUESTS = 20
MAX_CONCURRENT_IMAGES = 10

def carregar_eans_existentes():
    """
    Carrega todos os EANs já processados do arquivo CSV para cache em memória
    """
    global EANS_PROCESSADOS
    try:
        if not os.path.isfile(NM_ARQUIVO):
            return
        
        print("📋 Carregando EANs já processados...")
        with open(NM_ARQUIVO, mode='r', encoding='utf-8') as arquivo:
            leitor = csv.DictReader(arquivo, delimiter=';')
            for linha in leitor:
                ean = linha.get('EAN', '').strip()
                if ean:
                    EANS_PROCESSADOS.add(ean)
        
        print(f"✅ {len(EANS_PROCESSADOS)} EANs carregados no cache")
    except Exception as e:
        print(f"Erro ao carregar EANs existentes: {e}")

def produto_ja_existe(ean):
    """
    Verifica se o produto com o EAN já foi salvo (usando cache em memória)
    """
    return ean in EANS_PROCESSADOS

async def baixar_imagem_async(session, url_imagem, ean_produto, idx_imagem=0):
    """
    Baixa uma imagem da URL fornecida de forma assíncrona
    """
    try:
        if not url_imagem:
            print(f"  ⚠️ URL vazia para EAN {ean_produto}")
            return ""
        
        # Verifica se é placeholder (não baixa)
        if 'vtexassets.com' in url_imagem:
            print(f"  ⏭️ Pulando placeholder para EAN {ean_produto}: {url_imagem}")
            return ""
        
        # Cria o diretório se não existir
        Path(DIRETORIO_IMAGENS).mkdir(exist_ok=True)
        
        # Faz o download da imagem de forma assíncrona
        async with session.get(url_imagem, timeout=30) as response:
            if response.status != 200:
                print(f"  ❌ Erro HTTP {response.status} para EAN {ean_produto}: {url_imagem}")
                return ""
            
            content = await response.read()
            
            # Verifica se o conteúdo não está vazio
            if len(content) < 100:  # Imagens válidas são maiores que 100 bytes
                print(f"  ⚠️ Conteúdo muito pequeno para EAN {ean_produto}: {len(content)} bytes")
                return ""
        
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
            f.write(content)
        
        print(f"  📸 Imagem salva: {nome_arquivo} ({len(content)} bytes)")
        return caminho_arquivo
        
    except Exception as e:
        print(f"  ❌ Erro ao baixar imagem para EAN {ean_produto}: {str(e)}")
        return ""

async def baixar_imagens_produto(session, urls_imagens, ean_produto):
    """
    Baixa todas as imagens de um produto de forma assíncrona
    """
    if not urls_imagens:
        print(f"  ⚠️ Nenhuma URL de imagem encontrada para EAN {ean_produto}")
        return []
    
    print(f"  📥 Baixando {len(urls_imagens)} imagem(ns) para EAN {ean_produto}...")
    
    # Cria semáforo para limitar concorrência de imagens
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_IMAGES)
    
    async def download_with_semaphore(url, idx):
        async with semaphore:
            return await baixar_imagem_async(session, url, ean_produto, idx)
    
    # Executa downloads em paralelo
    tasks = [download_with_semaphore(url, i) for i, url in enumerate(urls_imagens)]
    resultados = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filtra apenas resultados válidos
    caminhos_validos = [r for r in resultados if r and not isinstance(r, Exception)]
    
    if caminhos_validos:
        print(f"  ✅ {len(caminhos_validos)}/{len(urls_imagens)} imagem(ns) baixada(s) com sucesso para EAN {ean_produto}")
    else:
        print(f"  ❌ Nenhuma imagem foi baixada para EAN {ean_produto}")
    
    return caminhos_validos

def salvar_informacoes_produto(idx, total, info_produto, caminhos_imagens=None):
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

    # Remove URLs das imagens (já processadas)
    info_produto.pop('_urls_imagens', [])
    
    # Atualiza os campos de imagem com os caminhos locais
    if caminhos_imagens:
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
        
        # Adiciona o EAN ao cache para evitar reprocessamento
        EANS_PROCESSADOS.add(info_produto['EAN'])

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
    
    print(f"  🔍 EAN {dados['EAN']}: Encontradas {len(urls_imagens)} URLs de imagem")
    if urls_imagens:
        for i, url in enumerate(urls_imagens[:3]):  # Mostra apenas as primeiras 3
            print(f"    {i+1}: {url}")
        if len(urls_imagens) > 3:
            print(f"    ... e mais {len(urls_imagens) - 3} URLs")
    
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

async def processar_produto_async(session, url, idx, total):
    """
    Processa um produto de forma assíncrona
    """
    try:
        # Faz requisição para página do produto
        async with session.get(url, timeout=30) as response:
            if response.status != 200:
                return None
            pagina = await response.text()
        
        # Extrai productId
        padrao_id = re.search(r'productId["\']?\s*[:=]\s*["\']?(\d+)', pagina)
        if not padrao_id:
            return None
        
        product_id = padrao_id.group(1)
        url_request = f'https://www.saojoaofarmacias.com.br/api/catalog_system/pub/products/search/?fq=productId:{product_id}'
        
        # Faz requisição para API do produto
        async with session.get(url_request, timeout=30) as api_response:
            if api_response.status != 200:
                return None
            info_produto = await api_response.json()
        
        # Mapeia campos
        campos = mapear_campos(info_produto)
        
        # Verifica se o produto já existe ANTES de baixar imagens
        ean_produto = campos.get('EAN', '')
        if produto_ja_existe(ean_produto):
            print(f"{idx} de {total} - ⏭️ Produto {campos['Nome']} (EAN: {ean_produto}) já existe, pulando...")
            return None
        
        # Baixa imagens de forma assíncrona (só se o produto não existir)
        urls_imagens = campos.get('_urls_imagens', [])
        caminhos_imagens = await baixar_imagens_produto(session, urls_imagens, ean_produto)
        
        # Salva informações do produto
        salvar_informacoes_produto(idx, total, campos, caminhos_imagens)
        
        return campos
        
    except Exception as e:
        print(f"❌ Erro ao processar produto {idx}: {str(e)}")
        return None

async def processar_produtos_batch(session, urls_batch, start_idx, total):
    """
    Processa um lote de produtos de forma assíncrona
    """
    # Cria semáforo para limitar concorrência
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async def processar_com_semaphore(url, idx):
        async with semaphore:
            return await processar_produto_async(session, url, idx, total)
    
    # Executa processamento em paralelo
    tasks = [processar_com_semaphore(url, start_idx + i) for i, url in enumerate(urls_batch)]
    resultados = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Conta produtos processados com sucesso
    sucessos = sum(1 for r in resultados if r and not isinstance(r, Exception))
    print(f"✅ Lote processado: {sucessos}/{len(urls_batch)} produtos salvos")
    
    return sucessos

async def webscraping_async():
    """
    Função principal assíncrona para webscraping
    """
    # Carrega EANs já processados para cache em memória
    carregar_eans_existentes()
    
    # Configuração da sessão HTTP assíncrona
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
    timeout = aiohttp.ClientTimeout(total=60)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Busca sitemaps de produtos
        async with session.get(URL_SITEMAP) as response:
            if response.status != 200:
                print(f"Erro ao acessar sitemap: {response.status}")
                return
            
            content = await response.text()
            root = ET.fromstring(content)

        urls_produtos = []
        for sitemap in root.findall(".//{*}sitemap"):
            loc = sitemap.find("{*}loc")
            if loc is not None and "product-" in loc.text:
                urls_produtos.append(loc.text.strip())

        print(f"🔍 Encontrados {len(urls_produtos)} sitemaps de produtos")

        total_produtos_processados = 0
        
        for sitemap_url in urls_produtos:
            print(f"📋 Processando sitemap: {sitemap_url}")
            
            async with session.get(sitemap_url) as response:
                if response.status != 200:
                    print(f"Erro ao acessar sitemap: {response.status}")
                    continue
                
                try:
                    content = await response.text()
                    root = ET.fromstring(content)
                    urls = [loc.text for loc in root.iter() if 'loc' in loc.tag]
                    
                    if not urls:
                        continue
                    
                    print(f"📦 Processando {len(urls)} produtos do sitemap...")
                    
                    # Processa em lotes para melhor controle
                    batch_size = 50
                    for i in range(0, len(urls), batch_size):
                        batch = urls[i:i + batch_size]
                        sucessos = await processar_produtos_batch(session, batch, i + 1, len(urls))
                        total_produtos_processados += sucessos
                        
                        # Pequena pausa entre lotes para não sobrecarregar o servidor
                        await asyncio.sleep(0.5)
                    
                except ET.ParseError:
                    print("O conteúdo retornado não é um XML válido.")
                    continue

    print(f"🎉 Processamento concluído! Total de produtos processados: {total_produtos_processados}")

def webscraping():
    """
    Função wrapper para executar o webscraping assíncrono
    """
    asyncio.run(webscraping_async())

if __name__ == '__main__':
    webscraping()
