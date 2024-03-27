import dateutil

def get_date_and_market_name(filename):
    market = filename.split(' ')[0]
    date = dateutil.parser.parse(filename.split(market)[1].split('.bz2')[0])
    return (date, market)