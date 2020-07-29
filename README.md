# USA China Venture Capital Investment Graph
Over the course of two months, ~50,000 datapoints representing VC investment events were scraped from a popular Chinese investment information aggregator using BeautifulSoup, cleaned, and loaded into a MongoDB Database.
This data was compared with [publicly available data](https://data.crunchbase.com/docs/2013-snapshot) from crunchbase.com, representing the state of US VC investment up to 2013.

I performed comparative analysis of the two datasets, primary from a venture capital co-investment perspective. Co-investment graphs were built from events in which two VC firms invested in the same round of a company.
Graphs were generated for different years and for different investment rounds. Network analysis was applied to the dataset to determine the prevalence of co-investment amongst and between different VC tiers.

This repository contains code with which we performed the analysis and graph modeling.
