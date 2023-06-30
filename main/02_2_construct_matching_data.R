# initialize ----
rm(list = ls())
library(magrittr)
# load data ----
conversion_ratio_tons_to_TEU = 
  20 # 20 tons = 1 TEU
CIY_data <- 
  readRDS(file = "cleaned/operator_level_panel_data.rds")
HB_data <-
  readRDS(file = "cleaned/HB_data.rds") %>% 
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
    year
    ) %>% 
  dplyr::summarise(
    TEU =
      sum(TEU)
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
  readRDS(file = "cleaned/shipdetails_container_data.rds") %>% 
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
    year
  ) %>% 
  dplyr::summarise(
    TEU =
      sum(TEU)
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
  ) %>% 
  dplyr::filter(
    dummy_merged_until_data_period == 1
  ) %>% 
  dplyr::select(
    operator,
    end,
    merging_firm
  ) 

operator_level_entry_exit_merger_HB <-
  readr::read_csv("input/operator_level_entry_exit_merger_HBdata.csv") %>% 
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
    merging_firm
  ) 

operator_level_entry_exit_merger_IHS <-
  readr::read_csv("input/operator_level_entry_exit_merger_IHS.csv") %>% 
  dplyr::distinct(
    parent_company,
    .keep_all = TRUE
  ) %>% 
  dplyr::rename(
    operator = parent_company
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
    merging_firm
  ) 

# construct matching data ----
construct_matching_pair_year <-
  function(
    data,
    operator_level_entry_exit_merger
    ){
    observed_characteristics <-
      data %>% 
      #data %>% 
      dplyr::group_by(
        operator,
        year) %>% 
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
        seller_operator_age_normalized,
        seller_cumsum_TEU_normalized,
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
  ) %>% 
  tidyr::drop_na()
matching_pair_year_IHS <-
  construct_matching_pair_year(
    IHS_data,
    operator_level_entry_exit_merger_IHS
  )
matching_pair_year_HB <-
  construct_matching_pair_year(
    HB_data,
    operator_level_entry_exit_merger_HB
  ) %>% 
  tidyr::drop_na()

# save ----
saveRDS(matching_pair_year_CIY,
        file = "output/matching_pair_year_CIY.rds")
saveRDS(matching_pair_year_IHS,
        file = "output/matching_pair_year_IHS.rds")
saveRDS(matching_pair_year_HB,
        file = "output/matching_pair_year_HB.rds")