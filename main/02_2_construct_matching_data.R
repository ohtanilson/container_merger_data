# initialize ----
rm(list = ls())
library(magrittr)
# load data ----
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

# construct matching data ----

## CIY data ----
observed_characteristics_CIY <-
  CIY_data %>% 
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
      year - initial_year
  ) %>% 
  dplyr::distinct(
    operator,
    year,
    operator_age,
    cumsum_TEU
  )

matching_pair_year_CIY <-
  operator_level_entry_exit_merger_CIY %>% 
  dplyr::filter(
    dummy_merged_until_data_period == 1
  ) %>% 
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
    observed_characteristics_CIY,
    by = c("seller_name" = "operator",
           "end" = "year")
  ) %>% 
  dplyr::rename(
    seller_operator_age = operator_age,
    seller_cumsum_TEU = cumsum_TEU
  ) %>% 
  dplyr::left_join(
    observed_characteristics_CIY,
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
      matching_pair_year_CIY$seller_cumsum_TEU,
      matching_pair_year_CIY$buyer_cumsum_TEU
      ),
    na.rm = T
  )

max_age <-
  max(
    c(
      matching_pair_year_CIY$seller_operator_age,
      matching_pair_year_CIY$buyer_operator_age
    ),
    na.rm = T
  )

matching_pair_year_CIY <-
  matching_pair_year_CIY %>% 
  dplyr::mutate(
    seller_cumsum_TEU_normalized =
      seller_cumsum_TEU/max_cumsum_TEU,
    buyer_cumsum_TEU_normalized = 
      buyer_cumsum_TEU/max_cumsum_TEU,
    seller_operator_age_normalized =
      seller_operator_age/max_age,
    buyer_operator_age_normalized = 
      buyer_operator_age/max_age
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


matching_matrix_CIY <-
  matching_pair_year_CIY %>% 
  dplyr::select(
    seller_name,
    buyer_name,
    end
  ) %>% 
  tidyr::pivot_wider(
    names_from = buyer_name,
    values_from = end
  )



