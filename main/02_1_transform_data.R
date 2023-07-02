# initialize ----
rm(list = ls())
library(magrittr)
# load data ----
CIY_data <- 
  readRDS(file = "cleaned/operator_level_panel_data.rds")
IHS_data <-
  readRDS(file = "cleaned/shipdetails_container_data.rds")
HB_data <-
  readRDS(file = "cleaned/HB_data.rds")

operator_level_entry_exit_merger_CIY <-
  readRDS(file = "cleaned/operator_level_entry_exit_merger_CIY.rds")
operator_level_entry_exit_merger_IHS <-
  readRDS(file = "cleaned/operator_level_entry_exit_merger_IHS.rds")
operator_level_entry_exit_merger_HBdata <-
  readRDS(file = "cleaned/operator_level_entry_exit_merger_HBdata.rds")

HB_data <-
  HB_data %>% 
  dplyr::rename(
    operator = operator_name
  ) %>% 
  dplyr::mutate(
    TEU = 
      ifelse(
        is.na(TEU) == 1,
        0,
        TEU
      )
  ) %>% 
  dplyr::group_by(
    operator,
    year#,
    #flag_country
  ) %>% 
  dplyr::summarise(
    TEU =
      sum(TEU),
    sum_dw_per_operator_year =
      sum(dw, na.rm = TRUE)
  ) %>% 
  dplyr::ungroup() %>% 
  dplyr::group_by(
    operator
  ) %>% 
  dplyr::arrange(
    operator,
    year
  ) %>% 
  dplyr::mutate(
    cumsum_TEU =
      cumsum(TEU)
  ) %>% 
  dplyr::ungroup() 
IHS_data <-
  IHS_data %>% 
  dplyr::rename(
    operator = parent_company
  ) %>% 
  dplyr::mutate(
    TEU =
      gross_ton# * 
    #conversion_ratio_tons_to_TEU
  ) %>% 
  dplyr::mutate(
    TEU = 
      ifelse(
        is.na(TEU) == 1,
        0,
        TEU
      )
  ) %>% 
  dplyr::group_by(
    operator,
    year#,
    #parent_company_country_of_control_nat1
  ) %>% 
  dplyr::summarise(
    TEU =
      sum(TEU),
    sum_dw_per_operator_year =
      sum(dwt, na.rm = TRUE)
  ) %>% 
  dplyr::ungroup() %>% 
  dplyr::group_by(
    operator
  ) %>% 
  dplyr::arrange(
    operator,
    year
  ) %>% 
  dplyr::mutate(
    cumsum_TEU =
      cumsum(TEU)
  ) %>% 
  dplyr::ungroup() 

# extract data ----
## extract HB_data ----
colnames(HB_data)

targeted_HB_data <-
  HB_data %>% 
  # dplyr::group_by(
  #   operator,
  #   year
  # ) %>%
  # dplyr::mutate(
  #   sum_dw_per_operator_year =
  #     sum(dw, na.rm = TRUE)
  # ) %>%
  # dplyr::ungroup() %>%
  dplyr::distinct(
    year,
    operator,
    sum_dw_per_operator_year#,
    #flag_country
  ) %>% 
  dplyr::select(
    year,
    operator,
    sum_dw_per_operator_year#,
    #flag_country
  ) %>% 
  dplyr::mutate(
    data_source =
      "HB"
  )
## extract IHS_data ----
colnames(IHS_data)

targeted_IHS_data <-
  IHS_data %>% 
  # dplyr::group_by(
  #   operator,
  #   year
  # ) %>% 
  # dplyr::mutate(
  #   sum_dw_per_operator_year =
  #     sum(dwt, na.rm = TRUE)
  # ) %>% 
  # dplyr::ungroup() %>% 
  dplyr::distinct(
    year,
    operator,
    sum_dw_per_operator_year# ,
    #parent_company_country_of_control_nat1
  ) %>% 
  dplyr::select(
    year,
    operator,
    sum_dw_per_operator_year# ,
    #parent_company_country_of_control_nat1
  ) %>% 
  dplyr::mutate(
    data_source =
      "IHS"
  )
## extract CIY_data ----
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
    sum_dw_per_operator_year#,
    #market
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
    #"country_or_market",
    "data_source"
    )
colnames(targeted_IHS_data) <-
  c(
    "year",
    "operator",
    "dw",
    #"country_or_market",
    "data_source"
  )
colnames(targeted_CIY_data) <-
  c(
    "year",
    "operator",
    "dw",
    #"country_or_market",
    "data_source"
  )

# merge data ----
operator_panel_data <-
  rbind(
    targeted_HB_data,
    targeted_IHS_data,
    targeted_CIY_data
    )






# write unique_operator_list ----
unique_operator_list_HB_and_CIY <-
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

unique_operator_list_IHS <-
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
    #country_or_market,
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
  unique_operator_list_IHS %>%
  dplyr::distinct(
    merging_firm
  ) %>%
  dplyr::mutate(
    operator_rename =
      stringr::str_to_title(merging_firm)
  )

# substitute missing firm-year samples for merging covariates ----
## change CIY data ----

## change IHS data ----

## change HB_data ----
# Ocean Network Express (2019年からしかHBに載っていないのに、2018年にmerging firmとして載っている)
temp <-
  HB_data %>% 
  dplyr::filter(
    operator == "Ocean Network Express"
  ) %>% 
  dplyr::filter(
    year == 2019
  ) %>% 
  dplyr::mutate(
    year = 2018
  )
HB_data <-
  rbind(
    HB_data,
    temp
  )
# Swire(2011年だけHBに存在しないが、2012年にmerging firmとして載っている)→2012年を代入
temp <-
  HB_data %>% 
  dplyr::filter(
    operator == "Swire"
  ) %>% 
  dplyr::filter(
    year == 2012
  ) %>% 
  dplyr::mutate(
    year = 2011
  )
HB_data <-
  rbind(
    HB_data,
    temp
  )
# Suzhou Shimonoseki Ferry(2008-2022年にHBに存在するが、2007年にmerging firmとして載っている)→2008を代入
temp <-
  HB_data %>% 
  dplyr::filter(
    operator == "Suzhou Shimonoseki Ferry"
  ) %>% 
  dplyr::filter(
    year == 2008
  ) %>% 
  dplyr::mutate(
    year = 2007
  )
HB_data <-
  rbind(
    HB_data,
    temp
  )
# TAICANG CONTAINER LINES(2012-2022年にHBに存在するが、2011年にmerging firmとして載っている)→2012を代入
temp <-
  HB_data %>% 
  dplyr::filter(
    operator == "TAICANG CONTAINER LINES"
  ) %>% 
  dplyr::filter(
    year == 2012
  ) %>% 
  dplyr::mutate(
    year = 2011
  )
HB_data <-
  rbind(
    HB_data,
    temp
  )
# Pan Continental(HBにparent_companyとして存在しない) →PanConのことで表記ブレが問題
# temp <-
#   HB_data %>% 
#   dplyr::filter(
#     operator == "PanCon"
#   ) %>% 
#   dplyr::filter(
#     year == 2012
#   ) %>% 
#   dplyr::mutate(
#     year = 2011
#   )
# HB_data <-
#   rbind(
#     HB_data,
#     temp
#   )

# drop samples which is not observed in data ----

## change CIY data ----
#Hamburg Sudに関しては古いのでデータ取れず落とす
operator_level_entry_exit_merger_CIY <-
  operator_level_entry_exit_merger_CIY %>% 
  dplyr::filter(
    merging_firm != "Hamburg Sud" |
      is.na(merging_firm) == 1
  )

## change IHS data ----

## change HB_data ----
operator_level_entry_exit_merger_HBdata <-
  operator_level_entry_exit_merger_HBdata %>% 
  dplyr::filter(
    merging_firm != "ZEAMARINE" &
      merging_firm != "Hyoki Kaiun"
  ) %>% 
  # fix typo
  dplyr::filter(
    ifelse(
      merging_firm == "Ocean Network Express" &
        end == 2008,
      1,
      0
    ) == 0
  )




# save ----
saveRDS(HB_data,
        file = "output/HB_data.rds")
saveRDS(IHS_data,
        file = "output/IHS_data.rds")
saveRDS(CIY_data,
        file = "output/CIY_data.rds")
saveRDS(operator_level_entry_exit_merger_CIY,
        file = "output/operator_level_entry_exit_merger_CIY.rds")
saveRDS(operator_level_entry_exit_merger_HBdata,
        file = "output/operator_level_entry_exit_merger_HBdata.rds")
saveRDS(operator_level_entry_exit_merger_IHS,
        file = "output/operator_level_entry_exit_merger_IHS.rds")



write.csv(unique_operator_list_HB_and_CIY, 
          file = "cleaned/unique_operator_list_HB_and_CIY.csv")
write.csv(unique_operator_list_IHS, 
          file = "cleaned/unique_operator_list_IHS.csv")