# initialize ----
rm(list = ls())
library(magrittr)
# load data ----
HB_data <-
  readRDS(file = "cleaned/HB_data.rds")
shipdetails_container_data <-
  readRDS(file = "cleaned/shipdetails_container_data.rds")
CIY_data <- 
  readRDS(file = "cleaned/operator_level_panel_data.rds")
operator_level_entry_exit_merger_CIY <-
  readr::read_csv("input/operator_level_entry_exit_merger_CIY.csv") %>% 
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
  )

operator_level_entry_exit_merger_HBdata <-
  readr::read_csv("input/operator_level_entry_exit_merger_HBdata.csv") %>% 
  dplyr::distinct(
    operator_name,
    .keep_all = TRUE
  ) 

operator_level_entry_exit_merger_IHS <-
  readr::read_csv("input/operator_level_entry_exit_merger_IHS.csv") %>% 
  dplyr::distinct(
    parent_company,
    .keep_all = TRUE
  ) 


# extract data ----
colnames(HB_data)

targeted_HB_data <-
  HB_data %>% 
  dplyr::group_by(
    operator_name,
    year
  ) %>% 
  dplyr::mutate(
    sum_dw_per_operator_year =
      sum(dw, na.rm = TRUE)
  ) %>% 
  dplyr::ungroup() %>% 
  dplyr::distinct(
    year,
    operator_name,
    sum_dw_per_operator_year,
    flag_country
  ) %>% 
  dplyr::select(
    year,
    operator_name,
    sum_dw_per_operator_year,
    flag_country
  ) %>% 
  dplyr::mutate(
    data_source =
      "HB"
  )

colnames(shipdetails_container_data)

targeted_shipdetails_container_data <-
  shipdetails_container_data %>% 
  dplyr::group_by(
    parent_company,
    year
  ) %>% 
  dplyr::mutate(
    sum_dw_per_operator_year =
      sum(dwt, na.rm = TRUE)
  ) %>% 
  dplyr::ungroup() %>% 
  dplyr::distinct(
    year,
    parent_company,
    sum_dw_per_operator_year ,
    parent_company_country_of_control_nat1
  ) %>% 
  dplyr::select(
    year,
    parent_company,
    sum_dw_per_operator_year ,
    parent_company_country_of_control_nat1
  ) %>% 
  dplyr::mutate(
    data_source =
      "IHS"
  )

colnames(CIY_data)
targeted_CIY_data <-
  CIY_data %>% 
  dplyr::group_by(
    operator,
    year
  ) %>% 
  dplyr::mutate(
    cumsum_TEU_per_operator_year =
      sum(cumsum_TEU, na.rm = TRUE)
  ) %>% 
  dplyr::ungroup() %>% 
  # assume 10dwt = 1TEU
  dplyr::mutate(
    sum_dw_per_operator_year =
      cumsum_TEU_per_operator_year * 10
  ) %>% 
  dplyr::distinct(
    year,
    operator,
    sum_dw_per_operator_year,
    market
  )  %>% 
  dplyr::select(
    year,
    operator,
    sum_dw_per_operator_year,
    market
  ) %>% 
  dplyr::mutate(
    data_source =
      "CIY"
  )

  
colnames(targeted_HB_data) <-
  c(
    "year",
    "operator",
    "dw",
    "country_or_market",
    "data_source"
    )
colnames(targeted_shipdetails_container_data) <-
  c(
    "year",
    "operator",
    "dw",
    "country_or_market",
    "data_source"
  )
colnames(targeted_CIY_data) <-
  c(
    "year",
    "operator",
    "dw",
    "country_or_market",
    "data_source"
  )

# merge data ----
operator_panel_data <-
  rbind(
    targeted_HB_data,
    targeted_shipdetails_container_data,
    targeted_CIY_data
    )



unique_operator_name_list_HB_and_CIY <-
  operator_panel_data %>% 
  dplyr::filter(
    data_source == "HB" |
      data_source == "CIY"
  ) %>%
  dplyr::arrange(
    operator,
    year
    ) %>% 
  dplyr::distinct(
    operator,
    data_source,
    .keep_all = TRUE
  )

unique_operator_name_list_IHS <-
  operator_panel_data %>% 
  dplyr::filter(
    data_source == "IHS"
  ) %>% 
  dplyr::mutate(
    connection_HB_or_CIY =
      ifelse(year == 1990 | year == 2006,
             1,
             0)
  ) %>%
  dplyr::arrange(year) %>% 
  dplyr::distinct(
    operator,
    data_source,
    connection_HB_or_CIY,
    .keep_all = TRUE
  ) %>% 
  dplyr::select(
    year,
    data_source,
    country_or_market,
    operator,
    #connection_HB_or_CIY
  ) %>% 
  dplyr::arrange(
    operator
  ) %>% 
  dplyr::mutate(
    operator_rename =
      stringr::str_to_title(operator)
  ) %>% 
  dplyr::left_join(
    operator_level_entry_exit_merger_IHS,
    by = c("operator" = "parent_company")
  ) %>% 
  dplyr::select(
    year,
    data_source:operator_rename,
    `name in CIY`,
    `name in HB`,
    merging_firm#,
    #connection_HB_or_CIY
  ) %>% 
  dplyr::arrange(
    operator
  ) %>% 
  dplyr::mutate(
    `name in CIY` =
      ifelse(
        is.na(`name in CIY`) == 1,
        ifelse(year == 1990,
               year,
               NA),
        `name in CIY`
      ),
    `name in HB` =
      ifelse(
        is.na(`name in HB`) == 1,
        ifelse(year == 2006,
               year,
               NA),
        `name in HB`
      )
  )
unique_merging_firm_name_list <-
  unique_operator_name_list_IHS %>%
  dplyr::distinct(
    merging_firm
  ) %>%
  dplyr::mutate(
    operator_rename =
      stringr::str_to_title(merging_firm)
  )

# save ----

write.csv(unique_operator_name_list_HB_and_CIY, 
          file = "cleaned/unique_operator_name_list_HB_and_CIY.csv")
write.csv(unique_operator_name_list_IHS, 
          file = "cleaned/unique_operator_name_list_IHS.csv")