#!/usr/bin/env python

import argparse
import csv
import os
import pickle
import re
import time

import lxml.html
import mysql.connector
import selenium
import selenium.webdriver
import yaml


BASE = 'https://www.votebuilder.com'
URLS ={
    'login': '{}/Login.aspx?OIDF=1'.format(BASE),
    'search': '{}/QuickLookUp.aspx'.format(BASE),
}
HOME = os.path.expanduser('~')
FADDR = os.path.join(HOME, 'addr.csv')
FOUT = os.path.join(HOME, 'vanscrape.{srctype:}.csv')

HERE = os.path.realpath(os.path.dirname(__file__))
FDBQRYS = os.path.join(HERE, 'queries.sql.yaml')


def main(loginurl, searchurl, fvancredentials, addresslist, fout, drivertype, srctype='voter'):
    print('starting scrape')

    driver = build_and_auth(
        drivertype=drivertype, loginurl=loginurl, fvancredentials=fvancredentials
    )

    d = []
    for (i, address) in enumerate(addresslist):
        d += get_people(driver=driver, searchurl=searchurl, address=address, srctype=srctype)
        # temp save (overwrite) every 20 addresses, and recreate driver (memory)
        if i > 0 and i % 20 == 0:
            print('temporary dump of current state')
            with open('tmp.pkl', 'wb') as f:
                pickle.dump(d, f)

            driver.close()
            driver.quit()
            del(driver)
            driver = build_and_auth(
                drivertype=drivertype, loginurl=loginurl, fvancredentials=fvancredentials
            )

    headers = d[0].keys()
    with open(fout.format(srctype=srctype), 'w') as f:
        c = csv.DictWriter(f, fieldnames=headers)
        c.writeheader()
        c.writerows(d)


# webdriver factory
def build_and_auth(drivertype, loginurl, fvancredentials):
    print('building new driver and logging in')
    driver = load_driver(drivertype)
    driver.implicitly_wait(5)
    sign_in(driver=driver, loginurl=loginurl, fvancredentials=fvancredentials)
    return driver


def load_driver(drivertype='phantomjs'):
    if drivertype.lower() == 'phantomjs':
        driver = selenium.webdriver.PhantomJS(
            service_args=['--ignore-ssl-errors=true', '--ssl-protocol=any']
        )
        driver.set_window_size(1120, 550)
        return driver
    elif drivertype.lower() == 'firefox':
        return selenium.webdriver.Firefox()


# auth
def sign_in(driver, loginurl, fvancredentials):
    print('logging in')
    driver.get(loginurl)

    xpaths = {
        'un': '//input[@id="TextBoxUserName"]',
        'pw': '//input[@id="TextBoxPassword"]',
        'sub': '//input[@id="ctl00_ContentPlaceHolderVANPage_ButtonLogin"]',
    }
    creds = load_from_yaml(fvancredentials)
    
    unbox = driver.find_element_by_xpath(xpaths['un'])
    unbox.clear()
    unbox.send_keys(creds['username'])

    pwbox = driver.find_element_by_xpath(xpaths['pw'])
    pwbox.clear()
    pwbox.send_keys(creds['password'])

    driver.find_element_by_xpath(xpaths['sub']).click()


def load_from_yaml(fname):
    with open(fname, 'r') as f:
        return yaml.load(f)


# iterating through externally-source addresses
def csv_addresses(faddresses):
    with open(os.path.expanduser(faddresses), 'r') as f:
        return [line.strip() for line in f]


def db_addresses(fdbcredentials, fdbqrys=FDBQRYS):
    # load credentials and query info
    dbcred = load_from_yaml(fdbcredentials)
    dbqrys = load_from_yaml(fdbqrys)
    print('connecting to db')
    con = mysql.connector.connect(**dbcred)
    cur = con.cursor()
    print('executing qry')
    cur.execute(dbqrys['addressqry'])
    addWithCt = sorted(list(cur), key=lambda row: -row[1])
    addresses = [_[0].strip().lower() for _ in addWithCt]
    con.close()
    print('finished with db qry')
    return addresses


# search for address and possibly iterating through multiple pages
def get_people(driver, searchurl, address, srctype):
    print('scraping data for address: {}'.format(address))
    if driver.current_url != searchurl:
        if srctype == 'voter':
            dot = 'My Voters'
        elif srctype == 'volunteer':
            dot = 'My Campaign'
            driver.get(searchurl)
        else:
            raise ValueError('invalid source type {}'.format(srctype))
        driver.find_element_by_xpath('.//a[@data-original-title="{}"]'.format(dot)).click()
        driver.find_element_by_id('ctl00_ContentPlaceHolderVANPage_HyperLinkMenuQuickLookUp').click()

    xpaths = {
        'adr': '//input[@id="ctl00_ContentPlaceHolderVANPage_ctl00_TextBoxFilterStreetAddress"]',
        'sub': '//input[@id="ctl00_ContentPlaceHolderVANPage_ctl00_RefreshFilterButton"]',
    }

    driver.find_element_by_xpath(xpaths['adr']).clear()
    driver.find_element_by_xpath(xpaths['adr']).send_keys(address)

    driver.find_element_by_xpath(xpaths['sub']).click()

    d = []
    d += parse_table_data(driver=driver, address=address, srctype=srctype)
    npb = next_page_button(driver=driver)
    while npb is not None:
        print('sleeping 1s before loading next page')
        time.sleep(1)
        npb.click()
        d += parse_table_data(driver=driver, address=address, srctype=srctype)
        npb = next_page_button(driver=driver)

    print('{} records found at that address'.format(len(d)))
    return d


def parse_table_data(driver, address, srctype):
    """we can assume that the current page held by driver contains table 
    information
    
    """
    subd = []

    # looks redundant, I know. Table isn't available on page load, so We need to
    # load the table in selenium to take advantage of the implicit wait
    tabid = "ctl00_ContentPlaceHolderVANPage_gvList"
    tab = driver.find_element_by_id(tabid)

    root = lxml.html.fromstring(driver.page_source)
    table = root.get_element_by_id(tabid)

    # stupid effing empty header for stupid checkboxes...
    #headers = table.xpath('.//th[@scope="col"]/a/text()') + ['apt_address', 'vanid']
    headers = [_.text_content().strip() for _ in table.xpath('.//th[@scope="col"]')]
    headers += ['apt_address', 'vanid']
    if srctype == 'volunteer':
        headers.append('mycampaignid')
    # nuke spaces, make insert harder
    headers = [h.replace(' ', '_') for h in headers]
    for row in table.xpath('./tbody/tr[not(@class)]'):
        # van id
        try:
            vanid = re.search('VANID=(.*)', row.find('./td/a').attrib['href']).groups()[0]
        except:
            vanid = None

        # volunteer id
        if srctype == 'volunteer':
            try:
                mycampaignid = row.find('./td/follow-button').attrib['data-entity-id']
            except:
                mycampaignid = None
        coltext = [td.text_content().strip() for td in row.xpath('./td')] + [address, vanid]
        if srctype == 'volunteer':
            coltext.append(mycampaignid)
        if len(headers) != len(coltext):
            raise ValueError("lists of incomparable length should not be zipped!")

        rowdict = dict(zip(headers, coltext))

        # volunteers have some crappy columns, bruh
        rowdict.pop('', None)
        rowdict.pop('Follow', None)

        subd.append(dict(zip(headers, coltext)))

    return subd


def next_page_button(driver):
    """check if the current page contains a link to the next page's table"""
    npb = None

    try:
        # the current page is displayed as a span element; all other tabs are 
        # a:href elems
        currentTab = driver.find_element_by_xpath('.//table[@class="pagination"]/tbody/tr/td/span')
        ctint = int(currentTab.text)
        otherTabs = driver.find_elements_by_xpath('//table[@class="pagination"]/tbody/tr/td/a')
        for ot in otherTabs:
            if int(ot.text) == ctint + 1:
                npb = ot
    except:
        pass

    return npb

    
# publish results to db
def load_csv(fdbcredentials, fcsv=FOUT, srctype='voter', fdbqrys=FDBQRYS):
    """load the credential and connection info and insert the items in the csv"""
    dbcred = load_from_yaml(fdbcredentials)
    dbqrys = load_from_yaml(fdbqrys)

    print('connecting to db')
    con = mysql.connector.connect(**dbcred)
    cur = con.cursor()

    print('executing insert qry')
    with open(fcsv.format(srctype=srctype), 'r') as f:
        for (i, row) in enumerate(csv.DictReader(f)):
            row['scrapeID'] = i
            cur.execute(dbqrys['insertqry'][srctype], row)

    try:
        con.commit()
    except:
        cur._last_executed
    cur.close()
    con.close()
    print('insert complete')


def wipe_db(fdbcredentials, srctype='voter', fdbqrys=FDBQRYS):
    """drop everything in the table. be careful ;)"""
    dbcred = load_from_yaml(fdbcredentials)
    dbqrys = load_from_yaml(fdbqrys)

    print('connecting to db')
    con = mysql.connector.connect(**dbcred)
    cur = con.cursor()
    print('executing delete qry')

    if srctype == 'voter':
        tablename = 'ACDCMUBS2016.VAN_data_scrape'
    elif srctype == 'volunteer':
        tablename = 'ACDCMUBS2016.MyCampaign_data_scrape'
    else:
        raise ValueError("invalid source type {}".format(srctype))
    deleteqry = "DELETE FROM {};".format(tablename)

    cur.execute(deleteqry)
    con.commit()
    cur.close()
    con.close()
    print('table wipe complete')


# cmd line
def parse_args():                                                               
    """ Take a log file from the commmand line """                              
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    loginurl = "the van login url"
    parser.add_argument("-l", "--loginurl", help=loginurl, default=URLS['login'])

    searchurl = "the van search page url"
    parser.add_argument("-s", "--searchurl", help=searchurl, default=URLS['search'])

    fvancredentials = "the van user credentials file"
    parser.add_argument("-v", "--fvancredentials", help=fvancredentials)

    mode = "the mode of obtaining addresses (from file, from a db, etc)"
    parser.add_argument('-m', "--mode", help=mode, choices=('csv', 'db'), default='db')

    faddresses = "the file listing addresses to query (one addr per line)"
    parser.add_argument("--faddresses", help=faddresses, default=FADDR)

    fdbcredentials = "the path to a connection and credentials file for the database"
    parser.add_argument("--fdbcredentials", help=fdbcredentials)

    fdbqrys = "the path to the yaml file of queries to exectue"
    parser.add_argument("-q", "--fdbqrys", help=fdbqrys, default=FDBQRYS)

    fout = "the name of the output csv file"
    parser.add_argument("-o", "--fout", help=fout, default=FOUT)

    srctype = "the type of van record we are scraping"
    parser.add_argument(
        "-t", "--srctype", help=srctype, choices=('voter', 'volunteer'), default='voter'
    )

    drivertype = "the type of selenium webdriver to use"
    parser.add_argument("-d", "--drivertype", help=drivertype, default='phantomjs')

    wipe = "flag indicating we should wipe the table of srctype in the db specified by fdbcredentials"
    parser.add_argument('-w', "--wipe", help=wipe, action='store_true')

    publish = "flag indicating we should publish to the db specified by fdbcredentials"
    parser.add_argument('-p', "--publish", help=publish, action='store_true')

    return parser.parse_args()
    

if __name__ == "__main__":
    args = parse_args()
    if args.mode == 'csv':
        addresslist = csv_addresses(args.faddresses)
    elif args.mode == 'db':
        addresslist = db_addresses(args.fdbcredentials, args.fdbqrys)
    else:
        raise ValueError("improper address iterator mode selection")

    main(
        loginurl=args.loginurl,
        searchurl=args.searchurl,
        fvancredentials=args.fvancredentials,
        addresslist=addresslist,
        fout=args.fout,
        drivertype=args.drivertype,
        srctype=args.srctype
    )

    if args.wipe:
        wipe_db(
            fdbcredentials=args.fdbcredentials, 
            srctype=args.srctype, 
            fdbqrys=args.fdbqrys
        )

    if args.publish:
        load_csv(
            fdbcredentials=args.fdbcredentials, 
            fcsv=args.fout, 
            srctype=args.srctype,
            fdbqrys=args.fdbqrys
        )
