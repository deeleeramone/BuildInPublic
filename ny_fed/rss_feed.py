"""Python Connector for Federal Reserve Board RSS Feeds (https://www.federalreserve.gov/feeds/feeds.htm)"""

from typing import Optional
import pandas as pd
import re
import feedparser

__docformat__ = "numpy"

FEEDS = {
	'Announcements' : {
        'All Data Download Announcements':'https://www.federalreserve.gov/feeds/datadownload.xml',
        'Aggregate Reserves of Depository Institution and the Monetary Base':'https://www.federalreserve.gov/feeds/h3.xml',
        'Assets and Liabilities of Commercial Banks in the United State':'https://www.federalreserve.gov/feeds/h8.xml',
        'Charge-Off and Delinquency Rates':'https://www.federalreserve.gov/feeds/chgdel.xml',
        'Commercial Paper':'https://www.federalreserve.gov/feeds/cp.xml',
        'Consumer Credit':'https://www.federalreserve.gov/feeds/g19.xml',
        'Factors Affecting Reserve Balances':'https://www.federalreserve.gov/feeds/h41.xml',
        'Finance Companies':'https://www.federalreserve.gov/feeds/g20.xml',
        'Financial Obligation Ratios':'https://www.federalreserve.gov/feeds/for.xml',
        'Financial Accounts of the United States':'https://www.federalreserve.gov/feeds/z1.xml',
        'Financial Accounts Guide':'https://www.federalreserve.gov/feeds/fofguide.xml',
        'Foreign Exchange Rates':'https://www.federalreserve.gov/feeds/h10.xml',
        'Industrial Production and Capacity Utilization':'https://www.federalreserve.gov/feeds/g17.xml',
        'Money Stock Measures':'https://www.federalreserve.gov/feeds/h6.xml',
        'Policy Rates':'https://www.federalreserve.gov/feeds/prates.xml',
        'Selected Interest Rates':'https://www.federalreserve.gov/feeds/h15.xml',
        'Senior Credit Officer Opinion Survey on Dealer Financing Terms':'https://www.federalreserve.gov/feeds/scoos.xml',
        'Senior Loan Officer Opinion Survey on Bank Lending Practices':'https://www.federalreserve.gov/feeds/sloos.xml',
        'Survey of Terms of Business Lending':'https://www.federalreserve.gov/feeds/e2.xml',
    },
    'Other' : {
        'Credit and Liquidity Programs and the Balance Sheet':'https://www.federalreserve.gov/feeds/clp.xml',
        'Office of the Inspector General':'https://oig.federalreserve.gov/feeds/oig.xml',
        'Board Meetings':'https://www.federalreserve.gov/feeds/boardmeetings.xml',
        'Reporting Forms':'https://www.federalreserve.gov/feeds/reportforms-rss.xml',
        'Supervision and Regulation and Consumer Affairs Letters, Supervision Manuals':'https://www.federalreserve.gov/feeds/bankinginfo-rss.xml',
    },
    'Outstanding' : {
        'Commercial Paper Outstanding':'http://www.federalreserve.gov/feeds/Data/CP_OUTST.xml',
    },
    'Press Releases' : {
        'All Press Releases':'https://www.federalreserve.gov/feeds/press_all.xml',
        'Banking and Consumer Regulatory Policy':'https://www.federalreserve.gov/feeds/press_bcreg.xml',
        'Enforcement Actions':'https://www.federalreserve.gov/feeds/press_enforcement.xml',
        'Monetary Policy':'https://www.federalreserve.gov/feeds/press_monetary.xml',
        'Orders on Banking Applications':'https://www.federalreserve.gov/feeds/press_orders.xml',
        'Other Announcements':'https://www.federalreserve.gov/feeds/press_other.xml',
    },
    'Rates' : {
        'Commercial Paper':'https://www.federalreserve.gov/feeds/Data/CP_RATES.xml',
        'Foreign Exchange':'https://www.federalreserve.gov/feeds/data/H10_H10.XML',
        'Selected Interest Rates':'https://www.federalreserve.gov/feeds/Data/H15_H15.XML',
    },
    'Research' : {
        'All Federal Reserve Board Working Papers':'https://www.federalreserve.gov/feeds/working_papers.xml',
        'Finance and Economics Discussion Series':'https://www.federalreserve.gov/feeds/feds.xml',
        'International Finance Discussion Papers':'https://www.federalreserve.gov/feeds/ifdp.xml',
        'FEDS Notes':'https://www.federalreserve.gov/feeds/feds_notes.xml',
    },
    'Speeches' : {
        'All Speeches':'https://www.federalreserve.gov/feeds/speeches.xml',
        'All Testimony':'https://www.federalreserve.gov/feeds/testimony.xml',
    }
}

CATEGORIES = list(FEEDS.keys())

def get_feed(
    category: Optional[str] = "",
    feed: Optional[str] = "",
    ) -> pd.DataFrame:
    """Gets a specific RSS feed from the Federal Reserve Board. 

    Parameters
    ----------
    category: Optional[str] = 'Outstanding'
        Choices are: ['Announcements', 'Other', 'Outstanding', 'Press Releases', 'Rates', 'Research', 'Speeches']
    feed: str = ""
        The feed to get. Choices differ by category. Defaults to Commercial Paper Outstanding. See all via: FEEDS

    Returns
    -------
    rss_feed: pd.DataFrame
        DataFrame with the selected RSS feed.

    Examples
    --------
    >>> rates = get_feed(category = "Rates", feed = "Selected Interest Rates")

    >>> press = get_feed(category = "Press Releases", feed = "All Press Releases")

    >>> research = get_feed(category = "Research", feed = "All Federal Reserve Board Working Papers")
    
    Print all valid categories:
    
    >>> print(CATEGORIES)
    
    Show all available feeds for a specific category:
    
    >>> FEEDS['Press Releases']
    """

    if category == "":
        category = "Outstanding"

    if category not in CATEGORIES:
        print("Invalid category. Please enter one of the following: ", CATEGORIES)
        return

    if category == "Outstanding":
        data = feedparser.parse(FEEDS[category]['Commercial Paper Outstanding'])
        rss_feed = (
            pd.DataFrame.from_records(data.entries)
            .get(['updated','cb_coverage','cb_otherstatistic','link'])
        )
        for i in rss_feed.index:
            char = data.entries[i]['cb_otherstatistic'].find('\n')
            rss_feed.cb_otherstatistic[i] = data.entries[i]['cb_otherstatistic'][:char]
        rss_feed = rss_feed.rename(
            columns = 
                {'cb_coverage': 'Description',
                 'cb_otherstatistic': 'Amount ($M USD)',
                 'updated': 'Date',
                 'link': 'Link'},
            )
        rss_feed = rss_feed.set_index('Date')
        rss_feed.index = (
            pd.to_datetime(rss_feed.index)
            .strftime('%Y-%m-%d')
        )
        return rss_feed

    feeds = FEEDS[category]

    if category == "Rates":
        if feed == "":
            feed = "Selected Interest Rates"
        if feed not in feeds.keys():
            print("Invalid feed. Please enter one of the following: ", list(feeds.keys()))
            return

        data = feedparser.parse(FEEDS[category][feed])
        rss_feed = (
            pd.DataFrame.from_records(data.entries)
            .get(['updated','cb_coverage','cb_otherstatistic','link'])
        )
        for i in rss_feed.index:
            char = data.entries[i]['cb_otherstatistic'].find('\n')
            rss_feed.cb_otherstatistic[i] = data.entries[i]['cb_otherstatistic'][:char]

        cols = ['Date', 'Description','Rate','Link']
        rss_feed.columns = cols
        rss_feed = rss_feed.set_index('Date').sort_index(ascending = False)
        rss_feed.index = pd.DatetimeIndex(rss_feed.index).strftime('%Y-%m-%d')

    if category == "Announcements":
        if feed == "":
            feed = "All Data Download Announcements"

        if feed not in feeds.keys():
            print("Invalid feed. Please enter one of the following: ", list(feeds.keys()))
            return

        data = feedparser.parse(FEEDS[category][feed])
        rss_feed = pd.DataFrame(data.entries, columns = ['updated','title','summary','link'])
        rss_feed.summary = rss_feed.summary.str.replace("\n", " ", regex = False)
        rss_feed.summary = rss_feed.summary.str.replace('<[^<]+?>', '', regex = True)
        rss_feed = rss_feed.rename(
            columns={
                'updated':'Date',
                'title':'Title',
                'summary':'Summary',
                'link':'Link'
                }
            )
        rss_feed = rss_feed.set_index('Date').sort_index(ascending = False)

    if category == "Speeches":
        if feed not in feeds.keys():
            print("Invalid feed. Please enter one of the following: ", list(feeds.keys()))
            return

        if feed == "":
            feed = "All Speeches"

        rss_feed = feedparser.parse(FEEDS[category][feed])
        speeches = pd.DataFrame(rss_feed.entries, columns = ['published','title','summary','link'])
        speeches.summary = speeches.summary.str.replace('<[^<]+?>', '', regex = True).str.replace('&quot','').str.replace(';','')
        speeches.summary = speeches.summary.str.replace('&#8212;', '', regex = False)
        speeches.rename(columns={'published':'Date', 'title':'Title', 'summary':'Summary', 'link':'Link'}, inplace = True)
        rss_feed = speeches.copy()

    if category == "Press Releases":
        if feed == "":
            feed = "All Press Releases"

        if feed not in feeds.keys():
            print("Invalid feed. Please enter one of the following: ", list(feeds.keys()))
            return

        rss_feed = feedparser.parse(FEEDS[category][feed])
        press = pd.DataFrame(rss_feed.entries, columns = ['published','title','summary','link'])
        press = press.rename(columns={'published':'Date', 'title':'Title', 'summary':'Summary', 'link':'Link'})
        rss_feed = press.copy()

    if category == "Other":

        if feed not in feeds.keys():
            print("Invalid feed. Please enter one of the following: ", list(feeds.keys()))
            return

        if feed == "Supervision and Regulation and Consumer Affairs Letters, Supervision Manuals":
            rss_feed = 'Supervision and Regulation and Consumer Affairs Letters, Supervision Manuals'
            rss_feed = feedparser.parse(FEEDS[category][feed])
            rss_feed = pd.DataFrame(rss_feed.entries, columns = ['published','title','summary','link'])
            rss_feed.summary = rss_feed.summary.str.replace('&nbsp', '').str.replace(';',' ')
            rss_feed = rss_feed.rename(columns={'published':'Date', 'title':'Title', 'summary':'Summary', 'link':'Link'})

        elif "Reporting Forms" in feed:
            rss_feed = feedparser.parse(FEEDS[category][feed])
            rss_feed = pd.DataFrame(rss_feed.entries, columns = ['updated','title','summary','link'])
            rss_feed.summary = rss_feed.summary.str.replace('<[^<]+?>', '', regex = True)
            rss_feed = rss_feed.rename(columns={'updated':'Date', 'title':'Title', 'summary':'Summary', 'link':'Link'})

        elif feed == "Board Meetings":
            rss_feed = feedparser.parse(FEEDS[category][feed])
            rss_feed = pd.DataFrame(rss_feed.entries, columns = ['published','title','summary','link'])
            rss_feed.summary = rss_feed.summary.str.replace('<[^<]+?>', '', regex = True)
            rss_feed = rss_feed.rename(columns={'published':'Date', 'title':'Title', 'summary':'Summary', 'link':'Link'})

        elif "Credit and Liquidity Programs and the Balance Sheet" or "Office of the Inspector General" in feed:
            rss_feed = feedparser.parse(FEEDS[category][feed])
            rss_feed = pd.DataFrame(rss_feed.entries, columns = ['updated','cb_simpletitle','summary','link'])
            rss_feed.summary = rss_feed.summary.str.replace("\n", " ", regex = False)
            rss_feed.summary = rss_feed.summary.str.replace('<[^<]+?>', '', regex = True)
            rss_feed = rss_feed.rename(columns={'updated':'Date', 'cb_simpletitle':'Title', 'summary':'Summary', 'link':'Link'})


    if category == "Research":
        if feed == "":
            feed = 'All Federal Reserve Board Working Papers'

        if feed not in feeds.keys():
            print("Invalid feed. Please enter one of the following: ", list(feeds.keys()))
            return

        rss_feed = feedparser.parse(FEEDS[category][feed])
        rss_feed = pd.DataFrame(rss_feed.entries, columns = ['published','title','summary','link'])
        rss_feed.summary = rss_feed.summary.str.replace('<[^<]+?>', '', regex = True)
        rss_feed = rss_feed.rename(columns={'published':'Date', 'title':'Title', 'summary':'Summary', 'link':'Link'})

        #rss_feed = pd.DataFrame(rss_feed.entries, columns = ['published','title','summary','link'])
        #rss_feed = rss_feed.rename(columns={'published':'Date', 'title':'Title', 'summary':'Summary', 'link':'Link'})

    return rss_feed
