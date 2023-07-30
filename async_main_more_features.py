import pandas as pd
from multiprocessing import Pool
import argparse
import os
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from datetime import datetime


parser = argparse.ArgumentParser()

parser.add_argument('input', help='Path to table with taxid for genomes')
parser.add_argument('output', help='Path to output table')
parser.add_argument('-num_workers', help='Number of proceses to use', type=int)

args = parser.parse_args()


def img2tab(table):
    browser = webdriver.Firefox()
    browser.get('https://img.jgi.doe.gov/cgi-bin/m/main.cgi?section=GenomeSearch&page=searchForm')
    time.sleep(3)

    inputElem = browser.find_element(by=By.ID, value='myAutoInput')
    inputElem.send_keys(f'{table[1]}') #453591
    time.sleep(5)

    selection = Select(browser.find_element(By.ID, 'searchById'))
    selection.select_by_visible_text('NCBI Taxon ID')
    time.sleep(3)

    clicable = browser.find_element(by=By.ID, value='quickSubmit')
    clicable.click()
    time.sleep(3)

    try:
        continue_link = browser.find_element(By.PARTIAL_LINK_TEXT, f"{table[0].split(' ')[0]}")  # Можно из названия сделать 
        continue_link.click()
        time.sleep(3)
        
        text = browser.find_element(By.XPATH, "/html/body").text
        #tex_lst = text.split('\n')

        if 'Genome Publication' in text:
            article_click = browser.find_element(By.CSS_SELECTOR, '#plus_minus_span2')
            article_click.click()
            text = browser.find_element(By.XPATH, "/html/body").text
            tex_lst = text.split('\n')
            browser.close()
        
        else:
            tex_lst = text.split('\n')
            browser.close()

    except :
        tex_lst = ['']
        browser.close()

    taxa = (''.join([i.split('Lineage')[-1].strip() for i in tex_lst if 'GTDBTK Lineage' in i]))
    biot_relation = (''.join([i.split('Relationships')[-1].strip() for i in tex_lst if 'Biotic Relationships' in i]))
    cell_shape = (''.join([i.split('Shape')[-1].strip() for i in tex_lst if 'Cell Shape' in i]))
    culutre_type = (''.join([i.split(' ')[-1].strip() for i in tex_lst if 'Cultured' in i]))
    ecsosys_cat = (''.join([i.split('Category')[-1].strip() for i in tex_lst if 'Ecosystem Category' in i]))
    energy_source = (''.join([i.split('Source')[-1].strip() for i in tex_lst if 'Energy Source' in i]))
    motility = (''.join([i.split(' ')[-1].strip() for i in tex_lst if 'Motility' in i]))
    ox = (''.join([i.split('Requirement')[-1].strip() for i in tex_lst if 'Oxygen Requirement' in i]))
    gramm = (''.join([i.split('Staining')[-1].strip() for i in tex_lst if 'Gram Staining' in i]))
    seq_qual = (''.join([i.split('Level')[-1].split(':')[-1] for i in tex_lst if 'Sequencing Quality Level' in i]))
    checkM_compl = (''.join([i.split('Completeness')[-1].strip() for i in tex_lst if 'CheckM Completeness' in i]))
    checkM_contam = (''.join([i.split('Contamination')[-1].strip() for i in tex_lst if 'CheckM Contamination' in i]))
    temp_range = (''.join([i.split('Range')[-1].strip() for i in tex_lst if 'Temperature Range' in i]))
    article_title = (''.join([' '.join(i.split(' ')[1::]) for i in tex_lst if 'Title' in i]))
    article_pumbed_id = (''.join([i.split('ID')[-1].strip() for i in tex_lst if 'Pubmed ID' in i]))
    article_doi = (''.join([i.split('Doi')[-1].strip() for i in tex_lst if 'Doi' in i]))

    if len(taxa) > 0 and len(article_pumbed_id) != 0:
        print(f'{table[0]} \U0001F44D')

    elif len(taxa) > 0 and len(article_pumbed_id) == 0:
        print(f'{table[0]} \U0001F928')

    else:
        print(f'{table[0]} \U0001F44E')

    all_s = [table[0], table[1] ,taxa, biot_relation, cell_shape, culutre_type, 
            ecsosys_cat, energy_source, motility, ox, gramm,  
            seq_qual,  checkM_compl, checkM_contam,  temp_range, 
            article_title, article_pumbed_id, article_doi ]
    
    return all_s


if __name__ == '__main__':

    start_time = datetime.now()
    os.environ['MOZ_HEADLESS'] = '1'
    p = Pool(processes=args.num_workers)
    tab_data = pd.read_csv(fr'{args.input}')

    print('\n')
    print(f'Total number of microbes: {len(tab_data)} \U0001FAE3')
    print('\n')
    print('Legend: \n')
    print('\U0001F44D -- IMG has data about this microbe.')
    print('\U0001F44E -- IMG has not data about this microbe.')
    print('\U0001F928 -- IMG has data, however this is unpublished research.')
    print('\n')


    zipok = zip(tab_data['Organism Name'].to_list(), tab_data['taxid'].to_list())
    answ_data = p.map(img2tab, [i for i in zipok])

    df = pd.DataFrame()

    for idx, mic in enumerate(answ_data):
        df.loc[idx, 'Name'] = mic[0]
        df.loc[idx, 'taqxid'] = int(mic[1])
        df.loc[idx, 'GTDBTK Lineage'] = mic[2]
        df.loc[idx, 'Biotic Relationships'] = mic[3]
        df.loc[idx, 'Cell Shape'] = mic[4]
        df.loc[idx, 'Cultured'] = mic[5]
        df.loc[idx, 'Ecosystem Category'] = mic[6]
        df.loc[idx, 'Energy Source'] = mic[7]
        df.loc[idx, 'Motility'] = mic[8]
        df.loc[idx, 'Oxygen Requiremen'] = mic[9]
        df.loc[idx, 'Gram Staining'] = mic[10]
        df.loc[idx, 'Sequencing Quality Level'] = mic[11]
        df.loc[idx, 'CheckM Completeness'] = mic[12]
        df.loc[idx, 'CheckM Contamination'] = mic[13]
        df.loc[idx, 'Temperature Range'] = mic[14]
        df.loc[idx, 'Article Title'] = mic[15]
        df.loc[idx, 'Pubmed ID'] = mic[16]
        df.loc[idx, 'DOI'] = mic[17]

    df.to_csv(f'./{args.output}', index=False)

    end_time = datetime.now()

    print(f'\nWork Duration: {end_time - start_time} \U0001FAE8')