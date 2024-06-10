-- BASE Table considered for the Time Window Analysis

-- Create a new table analytics_scratch.Ashika_PPL_6 with auctions, show, user information
-- considered only  'US' origin shows and live show for the analysis

-- CREATE TABLE analytics_scratch.Ashika_PPL_6 AS
SELECT  distinct dw_auctions_cs.auction_id,
       dw_auctions_cs.start_at as auction_start_at, 
       cast(dw_auctions_cs.start_at as TIME) as auction_start_time, -- Converting start_at to TIME format for hour/minute analysis
       EXTRACT(DOW FROM dw_auctions_cs.start_at) as auction_day_of_week, -- Extracted Day of the week for the start of the auction
       dw_auctions_cs.object_id AS listing_id, 
       dw_auctions_cs.order_id, 
       dw_auctions_cs.party_id, 
       dw_shows.creator_id, 
       dw_users.joined_at, -- When the creator joined the platform
       dw_users_cs.live_show_host_activated_at, -- Activation time for becoming a live show host
       dw_users_cs.posh_party_host_activated_at, -- Activation time for becoming a posh party host
       dw_users.followers, 
       dw_users.home_domain, 
       dw_shows.show_id, 
       dw_shows.start_at, -- Start time of the show
       dw_listings.seller_id, --ID of the one who sells the auctioned listing
       dw_listings.category_v2, 
       dw_listings.brand, 
       CASE WHEN (dw_auctions_cs.party_id) IS NOT NULL THEN 'Yes' ELSE 'No' END AS IS_PPL -- Indicating whether the auction is part of a  PPL or not based on party_id in auctions table

FROM analytics.dw_auctions_cs     -- Joined shows, listings, user table to the auctions table to fetch the required columns
         LEFT JOIN analytics.dw_listings ON dw_auctions_cs.object_id = dw_listings.listing_id 
         LEFT JOIN analytics.dw_shows ON dw_auctions_cs.show_id = dw_shows.show_id 
         LEFT JOIN analytics.dw_users_cs ON dw_shows.creator_id = dw_users_cs.user_id 
         LEFT JOIN analytics.dw_users ON dw_shows.creator_id = dw_users.user_id 

WHERE dw_shows.origin_domain = 'us' -- filtering the US domain shows and live shows             
  AND (NOT (dw_shows.type = 'silent' or dw_shows.title ilike '%silent%') OR
       (dw_shows.type = 'silent' or dw_shows.title ilike '%silent%') IS NULL);

-- GRANT ALL ON analytics_scratch.Ashika_PPL_6 TO PUBLIC;


--------------------------------------------------------------------------------------
-- Code for QA


-- QC with the looker to confirm that the values extracted are matching
select (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_6.auction_start_at)::integer),
                             Ashika_PPL_6.auction_start_at)), 'YYYY-MM-DD')) AS auction_start_week,
       count(auction_id)
from analytics_scratch.Ashika_PPL_6
group by 1
having auction_start_week > '2024-01-01'
   and auction_start_week <= '2024-04-28'; -- Pulled last 17 complete weeks distinct auction count and validated with looker values

--------------------------------------------------------------------------------------

-- Overall Average ACR in Hourly Time Interval


-- This query calculates the overall average count of different metrics like Host count, show count, total auctions, total order items, and Auction Conversion Rate (ACR) for each hour time interval within a specified date range.

SELECT (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_6.auction_start_at)::integer), Ashika_PPL_6.auction_start_at)), 'YYYY-MM-DD')) AS auction_start_week,  -- Grouped based on Auction start week

       CASE        -- Assigned time interval for based on auction's start time. 
            WHEN Ashika_PPL_6.auction_start_time between '00:00:00' and '01:00:00' THEN '12am-1am'
            WHEN Ashika_PPL_6.auction_start_time between '01:00:00' and '02:00:00' THEN '1am-2am'
            WHEN Ashika_PPL_6.auction_start_time between '02:00:00' and '03:00:00' THEN '2am-3am'
            WHEN Ashika_PPL_6.auction_start_time between '03:00:00' and '04:00:00' THEN '3am-4am'
            WHEN Ashika_PPL_6.auction_start_time between '04:00:00' and '05:00:00' THEN '4am-5am'
            WHEN Ashika_PPL_6.auction_start_time between '05:00:00' and '06:00:00' THEN '5am-6am'
            WHEN Ashika_PPL_6.auction_start_time between '06:00:00' and '07:00:00' THEN '6am-7am'
            WHEN Ashika_PPL_6.auction_start_time between '07:00:00' and '08:00:00' THEN '7am-8am'
            WHEN Ashika_PPL_6.auction_start_time between '08:00:00' and '09:00:00' THEN '8am-9am'
            WHEN Ashika_PPL_6.auction_start_time between '09:00:00' and '10:00:00' THEN '9am-10am'
            WHEN Ashika_PPL_6.auction_start_time between '10:00:00' and '11:00:00' THEN '10am-11am'
            WHEN Ashika_PPL_6.auction_start_time between '11:00:00' and '12:00:00' THEN '11am-12pm'
            WHEN Ashika_PPL_6.auction_start_time between '12:00:00' and '13:00:00' THEN '12pm-1pm'
            WHEN Ashika_PPL_6.auction_start_time between '13:00:00' and '14:00:00' THEN '1pm-2pm'
            WHEN Ashika_PPL_6.auction_start_time between '14:00:00' and '15:00:00' THEN '2pm-3pm'
            WHEN Ashika_PPL_6.auction_start_time between '15:00:00' and '16:00:00' THEN '3pm-4pm'
            WHEN Ashika_PPL_6.auction_start_time between '16:00:00' and '17:00:00' THEN '4pm-5pm'
            WHEN Ashika_PPL_6.auction_start_time between '17:00:00' and '18:00:00' THEN '5pm-6pm'
            WHEN Ashika_PPL_6.auction_start_time between '18:00:00' and '19:00:00' THEN '6pm-7pm'
            WHEN Ashika_PPL_6.auction_start_time between '19:00:00' and '20:00:00' THEN '7pm-8pm'
            WHEN Ashika_PPL_6.auction_start_time between '20:00:00' and '21:00:00' THEN '8pm-9pm'
            WHEN Ashika_PPL_6.auction_start_time between '21:00:00' and '22:00:00' THEN '9pm-10pm'
            WHEN Ashika_PPL_6.auction_start_time between '22:00:00' and '23:00:00' THEN '10pm-11pm'
            WHEN Ashika_PPL_6.auction_start_time >= '23:00:00' THEN '11pm-12am'
            END
           AS time_interval,
            COUNT(distinct Ashika_PPL_6.creator_id)                                                          AS Host_count,   -- Unique Host count
           COUNT(distinct Ashika_PPL_6.show_id)                                                             AS show_count,    -- Unique Show count
           COUNT(auction_id)                                                                                AS total_auctions, -- Unique Auction Count
           COUNT(case when order_id is not null then auction_id else null end)                              AS total_order_items, -- Unique Order items
           (total_order_items * 100.0) / NULLIF(total_auctions, 0)                                          AS total_ACR.     -- ACR



from analytics_scratch.Ashika_PPL_6 group by 1,2 having  auction_start_week > '2024-01-01' and auction_start_week <= '2024-04-28'. 
--- Considering the data only from 2024-01-01 till 2024-04-2028 for analysis 
    order by 1 desc ,2 asc;




--------------------------------------------------------------------------------------

-- Average Hourly Time Window ACR by Host type

%sql
-- This query calculates the count of different metrics like Host count, show count, total auctions, total order items, and Auction Conversion Rate (ACR) for each hour time interval by host type within a specified date range.
select (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_6.auction_start_at)::integer), Ashika_PPL_6.auction_start_at)), 'YYYY-MM-DD')) AS auction_start_week,
           CASE
               WHEN (TO_CHAR( DATE(DATEADD(day, (0 - EXTRACT(DOW FROM posh_party_host_activated_at)::integer), posh_party_host_activated_at)), 'YYYY-MM-DD'))  <= (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_6.start_at)::integer), Ashika_PPL_6.start_at)), 'YYYY-MM-DD')) THEN 'Yes'
               ELSE 'No'
           END AS Is_PPL_Host,  -- Whether PPL host Activated or not based on the posh_party_host_activated_at. If it is not null and show started after PPL activation then it is PPL host , else Non PPL Host.
       CASE
            WHEN Ashika_PPL_6.auction_start_time between '00:00:00' and '01:00:00' THEN '12am-1am'
            WHEN Ashika_PPL_6.auction_start_time between '01:00:00' and '02:00:00' THEN '1am-2am'
            WHEN Ashika_PPL_6.auction_start_time between '02:00:00' and '03:00:00' THEN '2am-3am'
            WHEN Ashika_PPL_6.auction_start_time between '03:00:00' and '04:00:00' THEN '3am-4am'
            WHEN Ashika_PPL_6.auction_start_time between '04:00:00' and '05:00:00' THEN '4am-5am'
            WHEN Ashika_PPL_6.auction_start_time between '05:00:00' and '06:00:00' THEN '5am-6am'
            WHEN Ashika_PPL_6.auction_start_time between '06:00:00' and '07:00:00' THEN '6am-7am'
            WHEN Ashika_PPL_6.auction_start_time between '07:00:00' and '08:00:00' THEN '7am-8am'
            WHEN Ashika_PPL_6.auction_start_time between '08:00:00' and '09:00:00' THEN '8am-9am'
            WHEN Ashika_PPL_6.auction_start_time between '09:00:00' and '10:00:00' THEN '9am-10am'
            WHEN Ashika_PPL_6.auction_start_time between '10:00:00' and '11:00:00' THEN '10am-11am'
            WHEN Ashika_PPL_6.auction_start_time between '11:00:00' and '12:00:00' THEN '11am-12pm'
            WHEN Ashika_PPL_6.auction_start_time between '12:00:00' and '13:00:00' THEN '12pm-1pm'
            WHEN Ashika_PPL_6.auction_start_time between '13:00:00' and '14:00:00' THEN '1pm-2pm'
            WHEN Ashika_PPL_6.auction_start_time between '14:00:00' and '15:00:00' THEN '2pm-3pm'
            WHEN Ashika_PPL_6.auction_start_time between '15:00:00' and '16:00:00' THEN '3pm-4pm'
            WHEN Ashika_PPL_6.auction_start_time between '16:00:00' and '17:00:00' THEN '4pm-5pm'
            WHEN Ashika_PPL_6.auction_start_time between '17:00:00' and '18:00:00' THEN '5pm-6pm'
            WHEN Ashika_PPL_6.auction_start_time between '18:00:00' and '19:00:00' THEN '6pm-7pm'
            WHEN Ashika_PPL_6.auction_start_time between '19:00:00' and '20:00:00' THEN '7pm-8pm'
            WHEN Ashika_PPL_6.auction_start_time between '20:00:00' and '21:00:00' THEN '8pm-9pm'
            WHEN Ashika_PPL_6.auction_start_time between '21:00:00' and '22:00:00' THEN '9pm-10pm'
            WHEN Ashika_PPL_6.auction_start_time between '22:00:00' and '23:00:00' THEN '10pm-11pm'
            WHEN Ashika_PPL_6.auction_start_time >= '23:00:00' THEN '11pm-12am'
            END
           AS time_interval,
            COUNT(distinct Ashika_PPL_6.creator_id)                                                          AS Host_count,
           COUNT(distinct Ashika_PPL_6.show_id)                                                             AS show_count,
           COUNT(auction_id)                                                                                AS total_auctions,
           COUNT(case when order_id is not null then auction_id else null end)                              AS total_order_items,
           (total_order_items * 100.0) / NULLIF(total_auctions, 0)                                          AS total_ACR



from analytics_scratch.Ashika_PPL_6 group by 1,2,3 having  auction_start_week > '2024-01-01' and auction_start_week <= '2024-04-28'
--- Considering the data only from 2024-01-01 till 2024-04-2028 for analysis 
 order by 1 desc ,2 asc,3;


--------------------------------------------------------------------------------------

-- Average Hourly Time Window ACR by Show type


-- This query calculates the count of different metrics like Host count, show count, total auctions, total order items, and Auction Conversion Rate (ACR) for each hour time interval by show type within a specified date range.

select (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM auction_table.auction_start_at)::integer),
                             auction_table.auction_start_at)), 'YYYY-MM-DD')) AS auction_start_week,
       CASE
           WHEN (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM posh_party_host_activated_at)::integer),
                                      posh_party_host_activated_at)), 'YYYY-MM-DD')) <= (TO_CHAR(
                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM auction_table.start_at)::integer), auction_table.start_at)),
                   'YYYY-MM-DD'))
               THEN CASE                                     -- Used Nested Case to fetch show type
                        WHEN Show_Type = 'PPL Show' THEN 'PPL_Host_PPL_show'     -- If the PPL Host and PPL show then PPL_Host_PPL_show
                        WHEN Show_Type = 'Non PPL Show' THEN 'PPL_Host_Non_PPL_show' -- else PPL_Host_Non_PPL_show
               END
           ELSE
               CASE
                   WHEN Show_Type = 'Non PPL Show' THEN 'Non_PPL_Host_Non_PPL_show'. -- Non PPL Host and Non PPL Show then Non_PPL_Host_Non_PPL_show
                   END
           END                                                               AS Show_category,

       CASE
           WHEN auction_table.auction_start_time between '00:00:00' and '01:00:00' THEN '12am-1am'
           WHEN auction_table.auction_start_time between '01:00:00' and '02:00:00' THEN '1am-2am'
           WHEN auction_table.auction_start_time between '02:00:00' and '03:00:00' THEN '2am-3am'
           WHEN auction_table.auction_start_time between '03:00:00' and '04:00:00' THEN '3am-4am'
           WHEN auction_table.auction_start_time between '04:00:00' and '05:00:00' THEN '4am-5am'
           WHEN auction_table.auction_start_time between '05:00:00' and '06:00:00' THEN '5am-6am'
           WHEN auction_table.auction_start_time between '06:00:00' and '07:00:00' THEN '6am-7am'
           WHEN auction_table.auction_start_time between '07:00:00' and '08:00:00' THEN '7am-8am'
           WHEN auction_table.auction_start_time between '08:00:00' and '09:00:00' THEN '8am-9am'
           WHEN auction_table.auction_start_time between '09:00:00' and '10:00:00' THEN '9am-10am'
           WHEN auction_table.auction_start_time between '10:00:00' and '11:00:00' THEN '10am-11am'
           WHEN auction_table.auction_start_time between '11:00:00' and '12:00:00' THEN '11am-12pm'
           WHEN auction_table.auction_start_time between '12:00:00' and '13:00:00' THEN '12pm-1pm'
           WHEN auction_table.auction_start_time between '13:00:00' and '14:00:00' THEN '1pm-2pm'
           WHEN auction_table.auction_start_time between '14:00:00' and '15:00:00' THEN '2pm-3pm'
           WHEN auction_table.auction_start_time between '15:00:00' and '16:00:00' THEN '3pm-4pm'
           WHEN auction_table.auction_start_time between '16:00:00' and '17:00:00' THEN '4pm-5pm'
           WHEN auction_table.auction_start_time between '17:00:00' and '18:00:00' THEN '5pm-6pm'
           WHEN auction_table.auction_start_time between '18:00:00' and '19:00:00' THEN '6pm-7pm'
           WHEN auction_table.auction_start_time between '19:00:00' and '20:00:00' THEN '7pm-8pm'
           WHEN auction_table.auction_start_time between '20:00:00' and '21:00:00' THEN '8pm-9pm'
           WHEN auction_table.auction_start_time between '21:00:00' and '22:00:00' THEN '9pm-10pm'
           WHEN auction_table.auction_start_time between '22:00:00' and '23:00:00' THEN '10pm-11pm'
           WHEN auction_table.auction_start_time >= '23:00:00' THEN '11pm-12am'
           END
                                                                             AS time_interval,
       COUNT(distinct auction_table.creator_id)                              AS Host_count,
       COUNT(distinct auction_table.show_id)                                 AS show_count,
       COUNT(auction_id)                                                     AS total_auctions,
       COUNT(case when order_id is not null then auction_id else null end)   AS total_order_items,
       (total_order_items * 100.0) / NULLIF(total_auctions, 0)               AS total_ACR

-- Generated an enriched table auction_table with calculated Show_Type
from (select *,
             CASE        -- This table contains all the columns in Ashika_PPL_6 base table along with Show_type that is derived.
                 WHEN show_table.PPL_Listing_Count > 0 THEN 'PPL Show' -- 
                 ELSE 'Non PPL Show'
                 END AS Show_Type --Determine if a show is a PPL Show based on the presence of count of PPL Auctions
      from analytics_scratch.Ashika_PPL_6
               left join (SELECT show_id                                         as show_id1,
                                 SUM(CASE WHEN IS_PPL = 'Yes' THEN 1 ELSE 0 END) AS PPL_Listing_Count 
                                 -- grouped by shows and counted the PPL_auctions count, if the PPL Auctions Count is not 0, then there is atleast one PPL auction happened, then it is a PPL Show. Else Non PPL SHow
                          FROM analytics_scratch.Ashika_PPL_6
                          group by 1) as show_table
                         on show_table.show_id1 = Ashika_PPL_6.show_id) as auction_table
group by 1, 2, 3
having auction_start_week > '2024-01-01'
   and auction_start_week <= '2024-04-28'
order by 1 desc, 2 asc, 3;

--------------------------------------------------------------------------------------

 -- Average Hourly Time Window ACR by Auction type


-- This query calculates the count of different metrics like Host count, show count, total auctions, total order items, and Auction Conversion Rate (ACR) for each hour time interval by auction type within a specified date range.

select (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM auction_table.auction_start_at)::integer),
                             auction_table.auction_start_at)), 'YYYY-MM-DD')) AS auction_start_week,
       CASE
           WHEN (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM posh_party_host_activated_at)::integer),
                                      posh_party_host_activated_at)), 'YYYY-MM-DD')) <= (TO_CHAR(
                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM auction_table.start_at)::integer), auction_table.start_at)),
                   'YYYY-MM-DD'))
               THEN CASE -- Shows done by the PPL Host 
                        WHEN Show_Type = 'PPL Show' THEN -- Auction type done in a PPL show will be considered.

                   CASE
                       WHEN IS_PPL ='Yes' THEN 'PPL_Show_PPL_Auction' -- Used Nested Case to fetch Auction type within Host and Show Type
                       WHEN IS_PPL='No' THEN 'PPL_Show_Non_PPL_Auction'
                       END
                        WHEN Show_Type = 'Non PPL Show' THEN 'PPL_Host_Non_PPL_show_Auction'
               END
           ELSE
               CASE -- Handling auctions by non-PPL hosts in Non PPL Shows
                   WHEN Show_Type = 'Non PPL Show' THEN 'Non_PPL_Host_Non_PPL_show_Auction'
                   END
           END                                                               AS Auction_category,

       CASE  -- Bucketing auctions by their start time
           WHEN auction_table.auction_start_time between '00:00:00' and '01:00:00' THEN '12am-1am'
           WHEN auction_table.auction_start_time between '01:00:00' and '02:00:00' THEN '1am-2am'
           WHEN auction_table.auction_start_time between '02:00:00' and '03:00:00' THEN '2am-3am'
           WHEN auction_table.auction_start_time between '03:00:00' and '04:00:00' THEN '3am-4am'
           WHEN auction_table.auction_start_time between '04:00:00' and '05:00:00' THEN '4am-5am'
           WHEN auction_table.auction_start_time between '05:00:00' and '06:00:00' THEN '5am-6am'
           WHEN auction_table.auction_start_time between '06:00:00' and '07:00:00' THEN '6am-7am'
           WHEN auction_table.auction_start_time between '07:00:00' and '08:00:00' THEN '7am-8am'
           WHEN auction_table.auction_start_time between '08:00:00' and '09:00:00' THEN '8am-9am'
           WHEN auction_table.auction_start_time between '09:00:00' and '10:00:00' THEN '9am-10am'
           WHEN auction_table.auction_start_time between '10:00:00' and '11:00:00' THEN '10am-11am'
           WHEN auction_table.auction_start_time between '11:00:00' and '12:00:00' THEN '11am-12pm'
           WHEN auction_table.auction_start_time between '12:00:00' and '13:00:00' THEN '12pm-1pm'
           WHEN auction_table.auction_start_time between '13:00:00' and '14:00:00' THEN '1pm-2pm'
           WHEN auction_table.auction_start_time between '14:00:00' and '15:00:00' THEN '2pm-3pm'
           WHEN auction_table.auction_start_time between '15:00:00' and '16:00:00' THEN '3pm-4pm'
           WHEN auction_table.auction_start_time between '16:00:00' and '17:00:00' THEN '4pm-5pm'
           WHEN auction_table.auction_start_time between '17:00:00' and '18:00:00' THEN '5pm-6pm'
           WHEN auction_table.auction_start_time between '18:00:00' and '19:00:00' THEN '6pm-7pm'
           WHEN auction_table.auction_start_time between '19:00:00' and '20:00:00' THEN '7pm-8pm'
           WHEN auction_table.auction_start_time between '20:00:00' and '21:00:00' THEN '8pm-9pm'
           WHEN auction_table.auction_start_time between '21:00:00' and '22:00:00' THEN '9pm-10pm'
           WHEN auction_table.auction_start_time between '22:00:00' and '23:00:00' THEN '10pm-11pm'
           WHEN auction_table.auction_start_time >= '23:00:00' THEN '11pm-12am'
           END
                                                                             AS time_interval,
       COUNT(distinct auction_table.creator_id)                              AS Host_count,   -- Counting unique hosts
       COUNT(distinct auction_table.show_id)                                 AS show_count,   -- Counting unique shows
       COUNT(auction_id)                                                     AS total_auctions,  -- Counting unique auctions
       COUNT(case when order_id is not null then auction_id else null end)   AS total_order_items, -- Counting  order items
       (total_order_items * 100.0) / NULLIF(total_auctions, 0)               AS total_ACR -- Calculating ACR

-- Creating an enriched table including Show Type based on PPL Listing Count
from (select *,
             CASE
             -- Determining Show Type based on PPL auction count
                 WHEN show_table.PPL_Listing_Count > 0 THEN 'PPL Show'
                 ELSE 'Non PPL Show'
                 END AS Show_Type
      from analytics_scratch.Ashika_PPL_6
               left join (SELECT show_id                                         as show_id1,
                                 SUM(CASE WHEN IS_PPL = 'Yes' THEN 1 ELSE 0 END) AS PPL_Listing_Count
                          FROM analytics_scratch.Ashika_PPL_6
                          group by 1) as show_table
                         on show_table.show_id1 = Ashika_PPL_6.show_id) as auction_table
group by 1, 2, 3
--  Filtering data for auctions between specified dates
having auction_start_week > '2024-01-01'
   and auction_start_week <= '2024-04-28'

