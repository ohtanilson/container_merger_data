# initialize ----
rm(list = ls())
library(magrittr)
# load data ----
conversion_ratio_tons_to_TEU = 
  20 # 20 tons = 1 TEU
CIY_data <- 
  readRDS(file = "output/CIY_data.rds")
IHS_data <-
  readRDS(file = "output/IHS_data.rds")
HB_data <-
  readRDS(file = "output/HB_data.rds")
flag_country_operator_country_list <- 
  readr::read_csv(file = "input/flag_country_operator_country_list.csv")
IHS_data_flag_country_name <-
  readRDS(file = "cleaned/shipdetails_container_data.rds") %>% 
  dplyr::distinct(
    parent_company,
    .keep_all = TRUE
  ) %>% 
  dplyr::select(
    parent_company,
    parent_company_country_of_domicile
  ) %>% 
  tidyr::drop_na()
HB_data_flag_country_name <-
  readRDS(file = "cleaned/HB_data.rds") %>% 
  dplyr::distinct(
    operator_name,
    .keep_all = TRUE
  ) %>% 
  dplyr::select(
    operator_name,
    flag_country
  ) %>% 
  tidyr::drop_na()

operator_level_entry_exit_merger_CIY <-
  readRDS(file = "output/operator_level_entry_exit_merger_CIY.rds")
operator_level_entry_exit_merger_IHS <-
  readRDS(file = "output/operator_level_entry_exit_merger_IHS.rds")
operator_level_entry_exit_merger_HBdata <-
  readRDS(file = "output/operator_level_entry_exit_merger_HBdata.rds")

unique_operator_name_list_matching_pair_year_IHS <- 
  readr::read_csv("input/unique_operator_name_list_matching_pair_year_IHS.csv") %>% 
  dplyr::distinct(
    seller_name,
    buyer_name,
    modified_seller_name_in_IHS,
    modified_buyer_name_in_IHS,
    merged_by_noncontainer_firm_or_rename_dummy
  )
operator_level_entry_exit_merger_CIY <-
  operator_level_entry_exit_merger_CIY %>%
  dplyr::distinct(
    operator,
    state,
    .keep_all = TRUE
  ) %>%
  dplyr::mutate(
    dummy_merged_until_data_period =
      ifelse(
        # this variable is manually added in csv
        is.na(dummy_merged_by_data_period1990) == 1,
        0,
        1
      )
  ) %>%
  dplyr::filter(
    dummy_merged_until_data_period == 1
  ) %>%
  dplyr::select(
    operator,
    end,
    merging_firm,
    matching_type
  )

operator_level_entry_exit_merger_IHS <-
  operator_level_entry_exit_merger_IHS %>%
  dplyr::distinct(
    parent_company,
    .keep_all = TRUE
  ) %>%
  dplyr::rename(
    operator = parent_company
  ) %>%
  # dplyr::mutate(
  #   dummy_merged_until_data_period =
  #     ifelse(
  #       # this variable is manually added in csv
  #       is.na() == 1,
  #       0,
  #       1
  #     )
  # ) %>%
  dplyr::filter(
    dummy_merged_by_data_period == 1
  ) %>%
  dplyr::select(
    operator,
    end,
    merging_firm,
    matching_type
  )

operator_level_entry_exit_merger_HBdata <-
  operator_level_entry_exit_merger_HBdata %>%
  dplyr::distinct(
    operator_name,
    .keep_all = TRUE
  ) %>%
  dplyr::rename(
    operator = operator_name
  ) %>%
  dplyr::mutate(
    dummy_merged_until_data_period =
      ifelse(
        # this variable is manually added in csv
        is.na(merging_firm) == 1,
        0,
        1
      )
  ) %>%
  dplyr::filter(
    dummy_merged_until_data_period == 1
  ) %>%
  dplyr::select(
    operator,
    end,
    merging_firm,
    matching_type
  )

# rename merging firm ----

## IHS data ----
operator_level_entry_exit_merger_IHS <-
  operator_level_entry_exit_merger_IHS %>% 
  dplyr::left_join(
    unique_operator_name_list_matching_pair_year_IHS %>% 
      dplyr::distinct(
        buyer_name,
        modified_buyer_name_in_IHS,
        merged_by_noncontainer_firm_or_rename_dummy
      ),
    by = c("merging_firm" = "buyer_name")
  ) %>% 
  dplyr::filter(
    is.na(merged_by_noncontainer_firm_or_rename_dummy) ==
      1
  ) %>%
  dplyr::select(
    - merging_firm,
    - merged_by_noncontainer_firm_or_rename_dummy
  ) %>% 
  dplyr::rename(
    merging_firm =
      modified_buyer_name_in_IHS
  )


# construct matching data ----
construct_matching_pair_year <-
  function(
    data,
    operator_level_entry_exit_merger
    ){
    observed_characteristics <-
      data %>% 
      dplyr::group_by(
        operator,
        year
        ) %>% 
      dplyr::summarize(
        cumsum_TEU =
          sum(cumsum_TEU, na.rm = T)
      ) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(
        initial_year =
          min(year)
      ) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(
        operator_age =
          (year - initial_year) + 1
      ) %>% 
      dplyr::distinct(
        operator,
        year,
        operator_age,
        cumsum_TEU
      )
    
    matching_pair_year <-
      operator_level_entry_exit_merger %>% 
      dplyr::arrange(desc(end)) %>% 
      dplyr::distinct(
        operator,
        .keep_all = T
      ) %>% 
      dplyr::select(
        operator,
        end,
        matching_type,
        merging_firm
      ) %>% 
      dplyr::arrange(end) %>% 
      dplyr::rename(
        seller_name =
          operator,
        buyer_name =
          merging_firm
      ) %>% 
      dplyr::left_join(
        observed_characteristics,
        by = c("seller_name" = "operator",
               "end" = "year")
      ) %>% 
      dplyr::rename(
        seller_operator_age = operator_age,
        seller_cumsum_TEU = cumsum_TEU
      ) %>% 
      dplyr::left_join(
        observed_characteristics,
        by = c("buyer_name" = "operator",
               "end" = "year")
      ) %>% 
      dplyr::rename(
        buyer_operator_age = operator_age,
        buyer_cumsum_TEU = cumsum_TEU
      ) %>% 
      dplyr::filter(
        is.na(buyer_name) == 0 &
          is.na(seller_name) == 0
      )
    max_cumsum_TEU <-
      max(
        c(
          matching_pair_year$seller_cumsum_TEU,
          matching_pair_year$buyer_cumsum_TEU
        ),
        na.rm = T
      )
    
    max_age <-
      max(
        c(
          matching_pair_year$seller_operator_age,
          matching_pair_year$buyer_operator_age
        ),
        na.rm = T
      )
    
    matching_pair_year <-
      matching_pair_year %>% 
      dplyr::mutate(
        seller_cumsum_TEU_normalized =
          (seller_cumsum_TEU/max_cumsum_TEU),
        buyer_cumsum_TEU_normalized = 
          (buyer_cumsum_TEU/max_cumsum_TEU),
        seller_operator_age_normalized =
          (seller_operator_age/max_age),
        buyer_operator_age_normalized = 
          (buyer_operator_age/max_age)
      ) %>% 
      dplyr::select(
        seller_name,
        buyer_name,
        end,
        matching_type,
        seller_operator_age,
        seller_cumsum_TEU,
        seller_operator_age_normalized,
        seller_cumsum_TEU_normalized,
        buyer_operator_age,
        buyer_cumsum_TEU,
        buyer_operator_age_normalized,
        buyer_cumsum_TEU_normalized
      ) 
    
    # matching_matrix <-
    #   matching_pair_year %>% 
    #   dplyr::select(
    #     seller_name,
    #     buyer_name,
    #     end
    #   ) %>% 
    #   tidyr::pivot_wider(
    #     names_from = buyer_name,
    #     values_from = end
    #   )
    return(matching_pair_year)
  }

## CIY data ----
matching_pair_year_CIY <-
  construct_matching_pair_year(
    CIY_data,
    operator_level_entry_exit_merger_CIY
  ) #%>% 
  #tidyr::drop_na()
matching_pair_year_IHS <-
  construct_matching_pair_year(
    IHS_data,
    operator_level_entry_exit_merger_IHS
  ) %>% 
  dplyr::mutate(
    buyer_operator_age_normalized = 
      ifelse(
        is.na(buyer_operator_age_normalized) == 1,
        1e-6, # NA at merging timing because of entry by merger
        buyer_operator_age_normalized
        ),
    buyer_cumsum_TEU_normalized =
      ifelse(
        is.na(buyer_cumsum_TEU_normalized) == 1,
        1e-6, # NA at merging timing because of entry by merger
        buyer_cumsum_TEU_normalized
      ),
    seller_operator_age_normalized = 
      ifelse(
        is.na(seller_operator_age_normalized) == 1,
        1e-6, # NA at merging timing because of entry by merger
        seller_operator_age_normalized
      ),
    seller_cumsum_TEU_normalized =
      ifelse(
        is.na(seller_cumsum_TEU_normalized) == 1,
        1e-6, # NA at merging timing because of entry by merger
        seller_cumsum_TEU_normalized
      )
  )
matching_pair_year_HBdata <-
  construct_matching_pair_year(
    HB_data,
    operator_level_entry_exit_merger_HBdata
  ) %>% 
  tidyr::drop_na()

unique_operator_name_list_matching_pair_year_IHS <-
  matching_pair_year_IHS %>% 
  dplyr::filter(
    is.na(seller_operator_age_normalized) == 1 |
      is.na(buyer_operator_age_normalized) == 1
  ) %>% 
  dplyr::select(
    - seller_cumsum_TEU_normalized,
    - buyer_cumsum_TEU_normalized
  ) %>% 
  dplyr::mutate(
    modified_buyer_name_in_IHS =
      NA,
    modified_seller_name_in_IHS =
      NA
  )


## get latitude and longtitude of flag countries ----
flag_country_operator_country_list_temp <-
  unique(
    c(
    matching_pair_year_CIY$seller_name,
    matching_pair_year_CIY$buyer_name,
    matching_pair_year_IHS$seller_name,
    matching_pair_year_IHS$buyer_name,
    matching_pair_year_HBdata$seller_name,
    matching_pair_year_HBdata$buyer_name
    )
  ) %>% 
  tibble::as_tibble() %>% 
  dplyr::rename(
    firm_name = value
  ) %>% 
  dplyr::distinct(firm_name) %>% 
  dplyr::left_join(
    IHS_data_flag_country_name,
    by = c("firm_name" = "parent_company")
  ) %>% 
  dplyr::left_join(
    HB_data_flag_country_name,
    by = c("firm_name" = "operator_name")
  ) %>% 
  dplyr::left_join(
    flag_country_operator_country_list,
    by = c("firm_name" = "firm_name")
  ) %>% 
  dplyr::select(
    firm_name,
    flag_country_geo_cepii
  )
country_geo_cepii <-
  cepiigeodist::geo_cepii %>% 
  dplyr::select(
    country,
    lat,
    lon
  ) %>% 
  dplyr::distinct(
    country,
    .keep_all = TRUE
  )
flag_country_operator_lat_long_list <-
  flag_country_operator_country_list_temp %>% 
  dplyr::left_join(
    country_geo_cepii,
    by = c("flag_country_geo_cepii" = "country")
  ) 
### CIY data ----
matching_pair_year_CIY <-
  matching_pair_year_CIY %>% 
  dplyr::left_join(
    flag_country_operator_lat_long_list,
    by = c("seller_name" = "firm_name")
  ) %>% 
  dplyr::rename(
    seller_flag_country_geo_cepii = flag_country_geo_cepii,
    seller_lon = lon,
    seller_lat = lat
  ) %>% 
  dplyr::left_join(
    flag_country_operator_lat_long_list,
    by = c("buyer_name" = "firm_name")
  ) %>% 
  dplyr::rename(
    buyer_flag_country_geo_cepii = flag_country_geo_cepii,
    buyer_lon = lon,
    buyer_lat = lat
  )
### IHS data ----
matching_pair_year_IHS <-
  matching_pair_year_IHS %>% 
  dplyr::left_join(
    flag_country_operator_lat_long_list,
    by = c("seller_name" = "firm_name")
  ) %>% 
  dplyr::rename(
    seller_flag_country_geo_cepii = flag_country_geo_cepii,
    seller_lon = lon,
    seller_lat = lat
  ) %>% 
  dplyr::left_join(
    flag_country_operator_lat_long_list,
    by = c("buyer_name" = "firm_name")
  ) %>% 
  dplyr::rename(
    buyer_flag_country_geo_cepii = flag_country_geo_cepii,
    buyer_lon = lon,
    buyer_lat = lat
  ) %>% 
  dplyr::filter(
    is.na(buyer_lat) == 0
  ) %>% 
  dplyr::filter(
    is.na(seller_lat) == 0
  )
### HB data ----
matching_pair_year_HBdata  <-
  matching_pair_year_HBdata %>% 
  dplyr::left_join(
    flag_country_operator_lat_long_list,
    by = c("seller_name" = "firm_name")
  ) %>% 
  dplyr::rename(
    seller_flag_country_geo_cepii = flag_country_geo_cepii,
    seller_lon = lon,
    seller_lat = lat
  ) %>% 
  dplyr::left_join(
    flag_country_operator_lat_long_list,
    by = c("buyer_name" = "firm_name")
  ) %>% 
  dplyr::rename(
    buyer_flag_country_geo_cepii = flag_country_geo_cepii,
    buyer_lon = lon,
    buyer_lat = lat
  )
# fixed merger year ----
## HB data ----
matching_pair_year_HBdata <-
  matching_pair_year_HBdata %>% 
  # fixed merger year
  dplyr::mutate(
    end = 
      ifelse(
        (
          ifelse(seller_name == "Hamburg Sud", 1, 0) == 
            1&
            ifelse(buyer_name == "Maersk", 1, 0) ==
            1 
        )  == 1,
        2018,
        end
      )
  ) %>% 
  # fixed merger year
  dplyr::mutate(
    end = 
      ifelse(
        (
          ifelse(seller_name == "APL", 1, 0) == 
            1&
            ifelse(buyer_name == "CMA-CGM", 1, 0) ==
            1 
        ) == 1,
        2017,
        end
      )
  )

# drop weird matching institutionally ----
## IHS data ----
matching_pair_year_IHS <-
  matching_pair_year_IHS %>% 
  dplyr::mutate(
    end =
      # last year inconsistency
      ifelse(end >= 2006, 2005, end)
  ) %>% 
  dplyr::filter(
    ifelse(seller_name == buyer_name, 1, 0) == 0 
  ) %>% 
  dplyr::filter(
    ifelse(seller_name == "BUSAN SHIPPING CO LTD", 1, 0) == 
      0
  ) %>% 
  # same firm different name
  dplyr::filter(
    (ifelse(seller_name == "MISC BERHAD", 1, 0) == 
      1&
      ifelse(buyer_name == "Malaysia Shipping Corp Sdn Bhd", 1, 0) ==
      1) == 0 
  ) %>% 
  # same firm different name
  dplyr::filter(
    (ifelse(seller_name == "CHINA MERCHANTS STEAM NAVIGATI", 1, 0) == 
       1&
       ifelse(buyer_name == "China Merchants Group", 1, 0) ==
       1) == 0 
  )
## HB data ----
matching_pair_year_HBdata <-
  matching_pair_year_HBdata %>% 
  dplyr::filter(
    end >= 2006
  ) 
matching_pair_year_HBdata <-
  matching_pair_year_HBdata %>% 
  # resolvent of route-level consortium
  dplyr::filter(
    ifelse(seller_name == "CMA-CGM/MSC", 1, 0) == 
      0
  ) %>% 
  # same firm different name
  dplyr::filter(
    (
      ifelse(seller_name == "PONL", 1, 0) == 
      1&
      ifelse(buyer_name == "Maersk", 1, 0) ==
      1 
    ) == 0
  ) %>% 
  # reconfiguration of SHK group
  dplyr::filter(
    (
      ifelse(seller_name == "S. S. Ferry/Orient Ferry", 1, 0) == 
        1&
        ifelse(buyer_name == "Suzhou Shimonoseki Ferry", 1, 0) ==
        1 
    ) == 0
  ) %>% 
  # already exist in IHS
  dplyr::filter(
    (
      ifelse(seller_name == "APL/PIL", 1, 0) == 
        1&
        ifelse(buyer_name == "CMA-CGM", 1, 0) ==
        1 
    ) == 0
  ) %>% 
  # rename in IHS
  dplyr::filter(
    (
      ifelse(seller_name == "Sealand Asia", 1, 0) == 
        1&
        ifelse(buyer_name == "Maersk", 1, 0) ==
        1 
    ) == 0
  )
matching_pair_year_HBdata <-
  matching_pair_year_HBdata %>% 
  dplyr::mutate(
    inconsistent_indicator =
      ifelse(
        (
          ifelse(seller_name == "Safmarine", 1, 0) == 
            1&
            ifelse(buyer_name == "Maersk", 1, 0) ==
            1 
          ) |
          (
            ifelse(seller_name == "Delmas", 1, 0) == 
              1&
              ifelse(buyer_name == "CMA-CGM", 1, 0) ==
              1 
          ) |
          (
            ifelse(seller_name == "ANL", 1, 0) == 
              1&
              ifelse(buyer_name == "CMA-CGM", 1, 0) ==
              1 
          ) |
          (
            ifelse(seller_name == "IRISL", 1, 0) == 
              1&
              ifelse(buyer_name == "Hafez Darya Arya Shipping Lines", 1, 0) ==
              1 
          )|
          (
            ifelse(seller_name == "Chongqing Marine", 1, 0) == 
              1&
              ifelse(buyer_name == "TAICANG CONTAINER LINES", 1, 0) ==
              1 
          ) |
          (
            ifelse(seller_name == "SEACON/T.S. Lines", 1, 0) ==
              1&
              ifelse(buyer_name == "T.S. Lines", 1, 0) ==
              1
          ) |
          (
            ifelse(seller_name == "Greater Bali-Hai", 1, 0) ==
              1&
              ifelse(buyer_name == "Swire ", 1, 0) ==
              1
          )
          # (
          #   ifelse(seller_name == "Hamburg Sud", 1, 0) == 
          #     1&
          #     ifelse(buyer_name == "Maersk", 1, 0) ==
          #     1 
          # )  
          == 1,
        1,
        0
      )
  )

# save ----
saveRDS(matching_pair_year_CIY,
        file = "output/matching_pair_year_CIY.rds")
saveRDS(matching_pair_year_IHS,
        file = "output/matching_pair_year_IHS.rds")
saveRDS(matching_pair_year_HBdata,
        file = "output/matching_pair_year_HBdata.rds")


# write.csv(flag_country_operator_country_list, 
#           file = "cleaned/flag_country_operator_country_list.csv")

write.csv(unique_operator_name_list_matching_pair_year_IHS, 
          file = "cleaned/unique_operator_name_list_matching_pair_year_IHS.csv")