from datetime import timedelta
from dateutil.parser import parse as date_parse


def datespan_tuple(startDate, endDate, delta=timedelta(days=1)):
    currentDate = startDate
    while currentDate < endDate:
        yield currentDate, min(currentDate + delta, endDate)
        currentDate += delta


def format_date(date):
    return date.strftime('%Y-%m-%d')  # time format used by CrowdTangle


class CTConfig:

    def __init__(self, scrape_status, query_type, search_params, chunk_days, current_chunk, chunks=()):
        self.scrape_status = scrape_status
        self.query_type = query_type
        self.search_params = search_params
        self.chunk_days = chunk_days
        self.current_chunk = current_chunk
        if not chunks:
            self.chunks = []
            # fill chunks list with {"startDate": [YYY-MM-DD], "endDate": [YYY-MM-DD]} entries
            start, end = date_parse(self.search_params["startDate"]), date_parse(self.search_params["endDate"])
            for current_date, next_date in datespan_tuple(start, end, delta=timedelta(days=self.chunk_days)):
                chunk = {"startDate": format_date(current_date), "endDate": format_date(next_date)}
                self.chunks.append(chunk)
        else:
            self.chunks = chunks
