-- Base Table for the Host Quality Analysis 

-- #considered only  'US' origin shows and live show for the analysis



-- DROP TABLE IF EXISTS analytics_scratch.Ashika_PPL_4;
-- CREATE TABLE analytics_scratch.Ashika_PPL_4 AS

SELECT  distinct dw_auctions_cs.auction_id,
        dw_auctions_cs.object_id AS listing_id,
       dw_auctions_cs.order_id,
       dw_auctions_cs.party_id,
       dw_shows.creator_id,
       dw_users.joined_at,  -- When the creator joined the platform
       dw_users_cs.live_show_host_activated_at, -- Activation time for becoming a live show host
       dw_users_cs.posh_party_host_activated_at,  -- Activation time for becoming a posh party host
       dw_users.followers,
       dw_users.home_domain,
       dw_shows.show_id, 
       dw_shows.start_at, -- Start time of the show
       dw_listings.seller_id, -- ID of the one who sells the auctioned listing
       dw_listings.category_v2,
       dw_listings.brand,
       CASE WHEN (dw_auctions_cs.party_id) IS NOT NULL THEN 'Yes' ELSE 'No' END AS IS_PPL -- Indicating whether the auction is part of a  PPL or not based on party_id in auctions table

FROM analytics.dw_auctions_cs. -- Joined shows, listings, user table to the auctions table to fetch the required columns
         LEFT JOIN analytics.dw_listings ON dw_auctions_cs.object_id = dw_listings.listing_id
         LEFT JOIN analytics.dw_shows ON dw_auctions_cs.show_id = dw_shows.show_id
         LEFT JOIN analytics.dw_users_cs ON dw_shows.creator_id = dw_users_cs.user_id
         LEFT JOIN analytics.dw_users ON dw_shows.creator_id = dw_users.user_id
         
-- filtering the US domain shows and live shows 
WHERE dw_shows.origin_domain = 'us'
  AND (NOT (dw_shows.type = 'silent' or dw_shows.title ilike '%silent%') OR
       (dw_shows.type = 'silent' or dw_shows.title ilike '%silent%') IS NULL);

-- GRANT ALL ON analytics_scratch.Ashika_PPL_4 TO PUBLIC;



---------------------------------------------------------------------------------------------

-- Query for Host Level, Show level and Auction Level Analysis



-- Query to analyze ACR,  count of host, shows, Auctions, Order Items  segmented by various factors such as host segment, host type, show type and Auction Type at show start week level.

-- Similar query is compiled using 'Union All' function with slight changes in the group by columns inorder to fetch unique host and show count.


-- First group based on the show start week
SELECT (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) AS show_start_week,
           coalesce(host_segments_gmv_start.host_segment_daily, 'Segment 1: No Sales') AS host_segment, -- 
           CASE
           ---- Categorize as PPL Host if a show host was activated as a posh party host before the week of the show, else Non PPL host
               WHEN posh_party_host_activated_at is not null and (TO_CHAR( DATE(DATEADD(day, (0 - EXTRACT(DOW FROM posh_party_host_activated_at)::integer), posh_party_host_activated_at)), 'YYYY-MM-DD'))  <= show_start_week THEN 'Yes'
               ELSE 'No'
           END AS Is_PPL_Host,  
           CASE
           -- Determine if a show is a PPL Show based on the presence of count of PPL Auctions
               WHEN PPL_show_table.PPL_Listing_Count > 0 THEN 'PPL Show'
               ELSE 'Non PPL Show'
           END AS Show_Type,
           CASE
           --Determine Auction type  based on the whether it is a PPL (has party_id) and with the PPL_type

               WHEN Is_PPL = 'No' and PPL_type is null THEN 'Non_PPl Auction'
               WHEN Is_PPL = 'Yes' and PPL_type = 'PPL' THEN 'Limited_PPL Auction'
               WHEN Is_PPL = 'Yes' and PPL_type = 'All_style_PPL' THEN 'All_Style_PPL Auction'
           END AS Is_PPL_Auction,
           -- ST auction or not based on whether the seller id equals creator_id
           CASE WHEN Ashika_PPL_4.seller_id <> Ashika_PPL_4.creator_id THEN 'Yes' ELSE 'No' END AS Is_ST, 
           COUNT(distinct Ashika_PPL_4.creator_id)                                                          AS Host_count, -- Unique count of host
           COUNT(distinct Ashika_PPL_4.show_id)                                                             AS show_count, -- Unique count of shows
           COUNT(auction_id)                                                                                AS total_auctions, -- Unique count of auction
           COUNT(case when order_id is not null then auction_id else null end)                              AS total_order_items, -- Unique order items
           (total_order_items * 100.0) / NULLIF(total_auctions, 0)                                          AS total_ACR --calculating the ACR
           FROM analytics_scratch.Ashika_PPL_4

           -- Joins PPL type column which determines whether the PPL auction is a 'ALL Style PPL' or 'Limited PPl'
          LEFT JOIN (SELECT DISTINCT party_id ,  
                        CASE
                            WHEN  brand IS NULL AND category IS NULL AND size_set_tag IS NULL AND color IS NULL AND subcategory IS NULL
                            AND condition IS NULL AND category_v2_str IS NULL AND category_v2_msg IS NULL  THEN 'All_style_PPL' 
                        -- When there is no criteria or  except dept rest of the criteria's is null then All Style PPL else 'PPL' which is 'Limited_PPL'
                            ELSE 'PPL'
                        END AS PPL_type
                    FROM analytics_scratch.ppl_criteria) AS all_party_style --PPL_criteria table contains all the party id and criteria associated with it
                    ON Ashika_PPL_4.party_id =all_party_style.party_id
                    -- Joins show type which determines if a show is a PPL Show based on the presence of count of PPL Auctions
            LEFT JOIN (SELECT show_id,
                             creator_id,
                             start_at,
                             SUM(CASE WHEN IS_PPL ='Yes' THEN 1 ELSE 0 END) AS PPL_Listing_Count
                      FROM analytics_scratch.Ashika_PPL_4 group by 1,2,3) AS PPL_show_table
                      ON Ashika_PPL_4.show_id = PPL_show_table.show_id and Ashika_PPL_4.start_at = PPL_show_table.start_at
                      -- Joins host segmentation table 'l365d_host_segment' which determines in which segment the host belongs to
           LEFT JOIN analytics_scratch.l365d_host_segment AS host_segments_gmv_start
                                   ON Ashika_PPL_4.creator_id = host_segments_gmv_start.host_id AND
                                   (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)),'YYYY-MM-DD')) > (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM host_segments_gmv_start.start_date)::integer),
                                   host_segments_gmv_start.start_date)), 'YYYY-MM-DD')) and (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer),
                                   Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) <= (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW
                                   FROM coalesce(host_segments_gmv_start.end_date, GETDATE()))::integer),
                                   coalesce(host_segments_gmv_start.end_date, GETDATE()))), 'YYYY-MM-DD'))
          GROUP BY 1,2,3,4,5,6

union all

-- This query part is similar to the above query part.
-- The only difference in this query is that we consider 'All' for show type, Is_PPL_Auction and IS_ST, as this helps to get the unique count of host and show in Host type level.

    SELECT (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) AS show_start_week,
           coalesce(host_segments_gmv_start.host_segment_daily, 'Segment 1: No Sales') AS host_segment,
           CASE
               WHEN  posh_party_host_activated_at is not null and (TO_CHAR( DATE(DATEADD(day, (0 - EXTRACT(DOW FROM posh_party_host_activated_at)::integer), posh_party_host_activated_at)), 'YYYY-MM-DD'))  <= show_start_week THEN 'Yes'
               ELSE 'No'
           END AS Is_PPL_Host,
           'All'AS Show_Type, -- Show type is considered as ALL
           'All' as Is_PPL_Auction,  --  Auction type is considered as ALL
           'All' as Is_ST,  -- ST is considered as ALL
           COUNT(distinct Ashika_PPL_4.creator_id)                                                          AS Host_count,
           COUNT(distinct Ashika_PPL_4.show_id)                                                             AS show_count,
           COUNT(auction_id)                                                                                AS total_auctions,
           COUNT(case when order_id is not null then auction_id else null end)                              AS total_order_items,
           (total_order_items * 100.0) / NULLIF(total_auctions, 0)                                          AS total_ACR
           FROM analytics_scratch.Ashika_PPL_4
           LEFT JOIN (SELECT DISTINCT party_id ,
                        CASE
                            WHEN  brand IS NULL AND category IS NULL AND size_set_tag IS NULL AND color IS NULL AND subcategory IS NULL
                            AND condition IS NULL AND category_v2_str IS NULL AND category_v2_msg IS NULL  THEN 'All_style_PPL'
                            ELSE 'PPL'
                        END AS PPL_type
                    FROM analytics_scratch.ppl_criteria) AS all_party_style
                    ON Ashika_PPL_4.party_id =all_party_style.party_id
           LEFT JOIN (SELECT show_id,
                             creator_id,
                             start_at,
                             SUM(CASE WHEN IS_PPL ='Yes' THEN 1 ELSE 0 END) AS PPL_Listing_Count
                      FROM analytics_scratch.Ashika_PPL_4 group by 1,2,3) AS PPL_show_table
                      ON Ashika_PPL_4.show_id = PPL_show_table.show_id and Ashika_PPL_4.start_at = PPL_show_table.start_at
           LEFT JOIN analytics_scratch.l365d_host_segment AS host_segments_gmv_start
                                   ON Ashika_PPL_4.creator_id = host_segments_gmv_start.host_id AND
                                   (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)),'YYYY-MM-DD')) > (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM host_segments_gmv_start.start_date)::integer),
                                   host_segments_gmv_start.start_date)), 'YYYY-MM-DD')) and (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer),
                                   Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) <= (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW
                                   FROM coalesce(host_segments_gmv_start.end_date, GETDATE()))::integer),
                                   coalesce(host_segments_gmv_start.end_date, GETDATE()))), 'YYYY-MM-DD'))
          GROUP BY 1,2,3,4,5,6

  union all
  -- This query part is similar to the above query part.
-- The only difference in this query is that we consider 'All' for Is_PPL_Auction and IS_ST, as this helps to get the unique count of host and show in Show type level.

    SELECT (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) AS show_start_week,
           coalesce(host_segments_gmv_start.host_segment_daily, 'Segment 1: No Sales') AS host_segment,
           CASE
               WHEN  posh_party_host_activated_at is not null and (TO_CHAR( DATE(DATEADD(day, (0 - EXTRACT(DOW FROM posh_party_host_activated_at)::integer), posh_party_host_activated_at)), 'YYYY-MM-DD'))  <= show_start_week THEN 'Yes'
               ELSE 'No'
           END AS Is_PPL_Host,
           CASE
               WHEN PPL_show_table.PPL_Listing_Count > 0 THEN 'PPL Show'
               ELSE 'Non PPL Show'
           END AS Show_Type,
           'All' as Is_PPL_Auction, --  Auction type is considered as ALL
           'All' AS Is_ST, -- ST is considered as ALL
           COUNT(distinct Ashika_PPL_4.creator_id)                                                          AS Host_count,
           COUNT(distinct Ashika_PPL_4.show_id)                                                             AS show_count,
           COUNT(auction_id)                                                                                AS total_auctions,
           COUNT(case when order_id is not null then auction_id else null end)                              AS total_order_items,
           (total_order_items * 100.0) / NULLIF(total_auctions, 0)                                          AS total_ACR
           FROM analytics_scratch.Ashika_PPL_4
           LEFT JOIN (SELECT DISTINCT party_id ,
                        CASE
                            WHEN  brand IS NULL AND category IS NULL AND size_set_tag IS NULL AND color IS NULL AND subcategory IS NULL
                            AND condition IS NULL AND category_v2_str IS NULL AND category_v2_msg IS NULL  THEN 'All_style_PPL'
                            ELSE 'PPL'
                        END AS PPL_type
                    FROM analytics_scratch.ppl_criteria) AS all_party_style
                    ON Ashika_PPL_4.party_id =all_party_style.party_id
           LEFT JOIN (SELECT show_id,
                             creator_id,
                             start_at,
                             SUM(CASE WHEN IS_PPL ='Yes' THEN 1 ELSE 0 END) AS PPL_Listing_Count
                      FROM analytics_scratch.Ashika_PPL_4 group by 1,2,3) AS PPL_show_table
                      ON Ashika_PPL_4.show_id = PPL_show_table.show_id and Ashika_PPL_4.start_at = PPL_show_table.start_at
           LEFT JOIN analytics_scratch.l365d_host_segment AS host_segments_gmv_start
                                   ON Ashika_PPL_4.creator_id = host_segments_gmv_start.host_id AND
                                   (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)),'YYYY-MM-DD')) > (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM host_segments_gmv_start.start_date)::integer),
                                   host_segments_gmv_start.start_date)), 'YYYY-MM-DD')) and (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer),
                                   Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) <= (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW
                                   FROM coalesce(host_segments_gmv_start.end_date, GETDATE()))::integer),
                                   coalesce(host_segments_gmv_start.end_date, GETDATE()))), 'YYYY-MM-DD'))
          GROUP BY 1,2,3,4,5,6

union all
  -- This query part is similar to the above query part.
-- The only difference in this query is that we consider 'All_Non_PPL_Auctions' and 'All_PPL_Auctions' for 'IS_PPL_Auction' and All' for  IS_ST, as this helps to get the unique count of host and show in PPL and Non Auction level. 
-- In 'All_PPL_Auctions' both 'Limited_PPL' and 'All_Style_PPL' is considered together.

        SELECT (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) AS show_start_week,
           coalesce(host_segments_gmv_start.host_segment_daily, 'Segment 1: No Sales') AS host_segment,
           CASE
               WHEN  posh_party_host_activated_at is not null and (TO_CHAR( DATE(DATEADD(day, (0 - EXTRACT(DOW FROM posh_party_host_activated_at)::integer), posh_party_host_activated_at)), 'YYYY-MM-DD'))  <= show_start_week THEN 'Yes'
               ELSE 'No'
           END AS Is_PPL_Host,
           CASE
               WHEN PPL_show_table.PPL_Listing_Count > 0 THEN 'PPL Show'
               ELSE 'Non PPL Show'
           END AS Show_Type,
           CASE
               WHEN Is_PPL = 'No' THEN 'All_Non_PPl Auction' -- It only contain the Non_PPL_Auctions
               WHEN Is_PPL = 'Yes' THEN 'All_PPL Auction' -- In 'All_PPL_Auctions' both 'Limited_PPL' and 'All_Style_PPL' is considered together.
           END AS Is_PPL_Auction,
           'All' AS Is_ST, -- ST is considered as ALL
           COUNT(distinct Ashika_PPL_4.creator_id)                                                          AS Host_count,
           COUNT(distinct Ashika_PPL_4.show_id)                                                             AS show_count,
           COUNT(auction_id)                                                                                AS total_auctions,
           COUNT(case when order_id is not null then auction_id else null end)                              AS total_order_items,
           (total_order_items * 100.0) / NULLIF(total_auctions, 0)                                          AS total_ACR
           FROM analytics_scratch.Ashika_PPL_4
           LEFT JOIN (SELECT DISTINCT party_id ,
                        CASE
                            WHEN  brand IS NULL AND category IS NULL AND size_set_tag IS NULL AND color IS NULL AND subcategory IS NULL
                            AND condition IS NULL AND category_v2_str IS NULL AND category_v2_msg IS NULL  THEN 'All_style_PPL'
                            ELSE 'PPL'
                        END AS PPL_type
                    FROM analytics_scratch.ppl_criteria) AS all_party_style
                    ON Ashika_PPL_4.party_id =all_party_style.party_id
           LEFT JOIN (SELECT show_id,
                             creator_id,
                             start_at,
                             SUM(CASE WHEN IS_PPL ='Yes' THEN 1 ELSE 0 END) AS PPL_Listing_Count
                      FROM analytics_scratch.Ashika_PPL_4 group by 1,2,3) AS PPL_show_table
                      ON Ashika_PPL_4.show_id = PPL_show_table.show_id and Ashika_PPL_4.start_at = PPL_show_table.start_at
           LEFT JOIN analytics_scratch.l365d_host_segment AS host_segments_gmv_start
                                   ON Ashika_PPL_4.creator_id = host_segments_gmv_start.host_id AND
                                   (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)),'YYYY-MM-DD')) > (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM host_segments_gmv_start.start_date)::integer),
                                   host_segments_gmv_start.start_date)), 'YYYY-MM-DD')) and (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer),
                                   Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) <= (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW
                                   FROM coalesce(host_segments_gmv_start.end_date, GETDATE()))::integer),
                                   coalesce(host_segments_gmv_start.end_date, GETDATE()))), 'YYYY-MM-DD'))
          GROUP BY 1,2,3,4,5,6

union all
  -- This query part is similar to the above query part.
-- The only difference in this query is that we consider 'All' for  IS_ST, as this helps to get the unique count of host and show in Auction type level (Limited_PPL_Auctions, All_style_PPL_Auctions, Non_PPL_Auctions).

           SELECT (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) AS show_start_week,
           coalesce(host_segments_gmv_start.host_segment_daily, 'Segment 1: No Sales') AS host_segment,
           CASE
               WHEN  posh_party_host_activated_at is not null and (TO_CHAR( DATE(DATEADD(day, (0 - EXTRACT(DOW FROM posh_party_host_activated_at)::integer), posh_party_host_activated_at)), 'YYYY-MM-DD'))  <= show_start_week THEN 'Yes'
               ELSE 'No'
           END AS Is_PPL_Host,
           CASE
               WHEN PPL_show_table.PPL_Listing_Count > 0 THEN 'PPL Show'
               ELSE 'Non PPL Show'
           END AS Show_Type,
           CASE
               WHEN Is_PPL = 'No' and PPL_type is null THEN 'Non_PPl Auction'
               WHEN Is_PPL = 'Yes' and PPL_type = 'PPL' THEN 'Limited_PPL Auction'
               WHEN Is_PPL = 'Yes' and PPL_type = 'All_style_PPL' THEN 'All_Style_PPL Auction'
           END AS Is_PPL_Auction,
           'All' AS Is_ST,  -- ST is considered as All
           COUNT(distinct Ashika_PPL_4.creator_id)                                                          AS Host_count,
           COUNT(distinct Ashika_PPL_4.show_id)                                                             AS show_count,
           COUNT(auction_id)                                                                                AS total_auctions,
           COUNT(case when order_id is not null then auction_id else null end)                              AS total_order_items,
           (total_order_items * 100.0) / NULLIF(total_auctions, 0)                                          AS total_ACR
           FROM analytics_scratch.Ashika_PPL_4
           LEFT JOIN (SELECT DISTINCT party_id ,
                        CASE
                            WHEN  brand IS NULL AND category IS NULL AND size_set_tag IS NULL AND color IS NULL AND subcategory IS NULL
                            AND condition IS NULL AND category_v2_str IS NULL AND category_v2_msg IS NULL  THEN 'All_style_PPL'
                            ELSE 'PPL'
                        END AS PPL_type
                    FROM analytics_scratch.ppl_criteria) AS all_party_style
                    ON Ashika_PPL_4.party_id =all_party_style.party_id
           LEFT JOIN (SELECT show_id,
                             creator_id,
                             start_at,
                             SUM(CASE WHEN IS_PPL ='Yes' THEN 1 ELSE 0 END) AS PPL_Listing_Count
                      FROM analytics_scratch.Ashika_PPL_4 group by 1,2,3) AS PPL_show_table
                      ON Ashika_PPL_4.show_id = PPL_show_table.show_id and Ashika_PPL_4.start_at = PPL_show_table.start_at
           LEFT JOIN analytics_scratch.l365d_host_segment AS host_segments_gmv_start
                                   ON Ashika_PPL_4.creator_id = host_segments_gmv_start.host_id AND
                                   (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer), Ashika_PPL_4.start_at)),'YYYY-MM-DD')) > (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM host_segments_gmv_start.start_date)::integer),
                                   host_segments_gmv_start.start_date)), 'YYYY-MM-DD')) and (TO_CHAR(
                                   DATE(DATEADD(day, (0 - EXTRACT(DOW FROM Ashika_PPL_4.start_at)::integer),
                                   Ashika_PPL_4.start_at)), 'YYYY-MM-DD')) <= (TO_CHAR(DATE(DATEADD(day, (0 - EXTRACT(DOW
                                   FROM coalesce(host_segments_gmv_start.end_date, GETDATE()))::integer),
                                   coalesce(host_segments_gmv_start.end_date, GETDATE()))), 'YYYY-MM-DD'))
          GROUP BY 1,2,3,4,5,6
 ORDER BY 1 DESC,2,3,4,5,6;
