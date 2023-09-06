# Script for scraping Vuokraovi data.
# author: Anna Hanslin 
# script can be used to scrap data from https://www.vuokraovi.com/ to SQL Server database
# please change connection string to your own (see pyodbc.connect)
# by default: Database=VUOKRAOVI;Trusted_Connection=yes
# table for storing data (truncates before every start)
'''
CREATE TABLE [dbo].[Apartments](
	[id] [int] NOT NULL,
	[get_date] [varchar](30) NULL,
	[link] [varchar](500) NULL,
	[img_link] [varchar](500) NULL,
	[building] [varchar](100) NULL,
	[size] [varchar](30) NULL,
	[planning] [varchar](100) NULL,
	[city] [varchar](100) NULL,
	[area] [varchar](30) NULL,
	[street_addr] [varchar](500) NULL,
	[rent] [varchar](30) NULL,
	[rent_curr] [varchar](30) NULL,
	[is_available] [int] NULL,
	[show_date] [varchar](30) NULL,
	[available_date] [varchar](30) NULL,
	[lessor] [varchar](100) NULL
) ON [PRIMARY]
GO
'''

#############################################################

# Import pyodbc for storing up values to SQL Server DB
import pyodbc 

# Import requests for making http requests.
import requests

# Import BeautifulSoup for handling html contents.
from bs4 import BeautifulSoup

# Import re for making regular experssions.
import re

# Import date for geting a parsing date.
import datetime


# number of pages to parse
html = requests.post('http://www.vuokraovi.com/vuokra-asunnot?page=1')
soup = BeautifulSoup(html.text)
pager_list = soup.find('div', {'class':'list-pager'}).find('ul').find_all('li')
page_num = int(pager_list[len(pager_list)-2].text)

#for test reasons max page number set up 100, remove if don't needed
if page_num > 100:
    page_num = 100

#connect to local SQL Server (by trusted connection)
#change connection string for connect to your server
cnxn = pyodbc.connect(r'Driver=SQL Server;Server=.\SQLEXPRESS;Database=VUOKRAOVI;Trusted_Connection=yes;')
cursor = cnxn.cursor()

#remove all data from destination table
cursor.execute('TRUNCATE TABLE dbo.Apartments')
cursor.commit()

for page in range(1, page_num+1):

    # request page data and make a 'soup'
    html = requests.post('http://www.vuokraovi.com/vuokra-asunnot?page='+str(page))
    soup = BeautifulSoup(html.text)

    #list of apartments on this page
    items = soup.find_all('div', {'class': 'list-item-container'})

    for item in items:
        apartment = dict()
        link = item.find('a',{'class': 'list-item-link'})['href']

        #get app id from the link
        m = re.search('(\d+)\?entryPoint', link)
        apartment['id'] = int(m.group(1)) 
        
        #put current date as a date
        apartment['get_date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        apartment['link'] = 'https://www.vuokraovi.com' + link
        apartment['img_link'] = ''
        about_flat_list = item.find('div', {'class': 'col-xs-7 col-sm-3 col-2'}).find_all('li')
        apartment['building'] = about_flat_list[0].find('span', {'class':'capitalize'}).text #'kerrostalo'
        apartment['size'] = about_flat_list[0].text.replace( apartment['building'] + ',', '').replace('m²','').strip().replace(',','.')#'41'
        apartment['planning'] = about_flat_list[1].text.strip()

        address_li = about_flat_list[2].text.strip().replace(u'\n', u' ').split(',')
        address_li = list(map(str.strip, address_li))
        
        #if region is specified
        if len(address_li) == 3: 
            apartment['city'] = address_li[0]
            apartment['area'] = address_li[1] 
            apartment['street_addr'] = address_li[2]
        #if region is not specified
        elif len(address_li) == 2:
            apartment['city'] = address_li[0] 
            apartment['area'] = ''
            apartment['street_addr'] = address_li[1]
        #if smth goes wrong put all address to the city field
        else:
            apartment['city'] = address_li[0] 
            apartment['area'] = ''
            apartment['street_addr'] =''

        apartment['rent'] = about_flat_list[3].text.strip().replace(u'\xa0', u' ').replace('€/kk','').replace(' ','').replace(',', '.')
        apartment['rent_curr'] = '€/kk'

        #stubs for future goals
        apartment['is_available'] = ''
        apartment['show_date'] = ''
        apartment['available_date'] = ''
        apartment['lessor'] = ''

        #send apartment to the database 
        cursor.execute("insert into dbo.Apartments( id, get_date, link, img_link , building , size , planning , city , area , street_addr , rent , rent_curr , is_available , show_date , available_date , lessor ) values (?, ?,?, ?,?, ?,?, ?,?, ?,?, ?,?, ?,?, ?)", apartment['id']
        ,apartment['get_date']
        ,apartment['link']
        ,apartment['img_link']               
        ,apartment['building']
        ,apartment['size']
        ,apartment['planning']
        ,apartment['city']
        ,apartment['area']
        ,apartment['street_addr']
        ,apartment['rent']
        ,apartment['rent_curr']
        ,apartment['is_available']
        ,apartment['show_date']
        ,apartment['available_date']
        ,apartment['lessor'])
        
        cnxn.commit()
        
    print(f'Pages has parsed {page}')
    
cnxn.close()