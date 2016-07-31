Wikipedia pageview statistics released into the public domain, provided by Domas Mituzas, 
collected and organized into a AWS public dataset by Data Wrangling LLC  
http://www.datawrangling.com

This dataset covers 16 months (October 01 2008 - February 12 2010) of hourly article
pageviews for Wikipedia (650 GB compressed / ~ 2.5 TB uncompressed) 

Each file is named with the date and time of collection (pagecounts-20090430-230000.gz)

Each line has 4 fields: projectcode, pagename, pageviews, bytes

en Barack_Obama 997 123091092
en Barack_Obama%27s_first_100_days 8 850127
en Barack_Obama,_Jr 1 144103
en Barack_Obama,_Sr. 37 938821
en Barack_Obama_%22HOPE%22_poster 4 81005
en Barack_Obama_%22Hope%22_poster 5 102081
en Barack_Obama_(comic_character) 1 11142
en Barack_Obama_(singer) 2 12110
en Barack_Obama_2009_presidential_inauguration 18 1413722
en Barack_Obama_Cabinet 1 70078
en Barack_Obama_Presidency 1 6046
en Barack_Obama_Sr. 11 230851
en Barack_Obama_Supreme_Court_candidates 1 26309
en Barack_Obama_citizenship_conspiracy_theories 17 816085
en Barack_Obama_election_victory_speech,_2008 7 125683

Many of the raw wikistats titles are percent-encoded (see http://en.wikipedia.org/wiki/Percent-encoding)
To match these with the page titles in the wikidump database, you can transform them as follows in python:

$ python
>>> import urllib
>>> escaped_title = '%22Klondike_Kate%22_Rockwell'
>>> print urllib.unquote_plus(escaped_title)
"Klondike_Kate"_Rockwell

$ grep '"Klondike_Kate"_Rockwell' page.txt 
6072468	0	"Klondike_Kate"_Rockwell		0	0	1	0.66477259946987033	20090225235249	273308879	3900


see http://dammit.lt/2007/12/10/wikipedia-page-counters/ for more details

-------------------------------------

original announcement:

[Wikitech-l] page counters
Domas Mituzas midom.lists at gmail.com
Mon Dec 10 12:03:37 UTC 2007

    * Previous message: [Wikitech-l] XML or CSV import?
    * Next message: [Wikitech-l] page counters
    * Messages sorted by: [ date ] [ thread ] [ subject ] [ author ]

Hello,

we have now some kind of 'what pages are visited' statistics. It  
isn't very trivial to separate exact pageviews, but this regular  
expression should does the rough job :)

urlre = re.compile('^http://([^\.]+)\.wikipedia.org/wiki/([^?]+)')

It is applied to our squid access-log stream and redirected to  
profiling agent (webstatscollector - then the hourly snapshots are  
written in very trivial format.
This can be used to both noticing strange activities, as well as  
spotting trends (specific events show up really nicely - let it be a  
movie premiere, a national holiday or any scandal :). Last March,  
when I was experimenting with it, it was impossible not to notice  
that "300" did hit theatres, St.Patrick's day revealed Ireland, and  
there was some crazy DDoS against us.

Anyway, log files for now are at:
http://dammit.lt/wikistats/
- didn't figure out yet retention policy, but as there're few gigs  
available, at least few weeks should be up.
A normal snapshot contains ~3.5M page titles and extracted is over  
100MB. Entries inside are grouped by project, and in semi-alphabetic  
order.

I'm experimenting with visualization software too, so if you have any  
ideas and are too lazy to implement - share them anyway :)

Cheers,
-- 
Domas Mituzas -- http://dammit.lt/ -- [[user:midom]]


