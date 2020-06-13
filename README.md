# Gig Guide
Project to expose an open dataset of local Australian gigs.

## Motivation

The Covid-19 pandemic has caused live music venues to close and/or limit their capacity until lockdown measures lift. This will have a long standing negative impact on arts and culture in cities across the world.

The project aims to give live music a boost by creating a central repository of live local gigs, with additional information on artists and locations. The end goal is for this dataset to serve a front-end application that allows users to easily search for gigs geospatially, by price, by genre, and supply as much additional information about artists as possible (links for their music, etc).

There are existing services that aggregate music events, such as eventfinder. These however focus on large events, often missing out on small, local gigs with little-to-no promotion budget. By scraping the web for smaller local events events we can give them exposure automatically rather than relying on artists to extensively promote their performances.

By creating a convenient place to search for gigs we're hoping to increase the attendance of local live music performances when the lockdown lifts.


## Codebase

python/extract contains web scraping and data extraction scripts for sourcing data.

sql contains the dbt project to transform data.

export contains scripts to export data into google cloud storage for consumption by end users.
