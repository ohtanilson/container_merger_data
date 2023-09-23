# initialize ----
rm(list = ls())
library(magrittr)
# load data ----
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

# fixed missing merger data ----
# operator_level_entry_exit_merger_HBdata <-
#   rbind(
#     operator_level_entry_exit_merger_HBdata,
#   c(
#     184,
#     "OOCL",
#     2005,
#     2018,
#     "COSCO",
#     NA
#     )
# )

  


# save ----
saveRDS(operator_level_entry_exit_merger_CIY,
        file = "cleaned/operator_level_entry_exit_merger_CIY.rds")
saveRDS(operator_level_entry_exit_merger_HBdata,
        file = "cleaned/operator_level_entry_exit_merger_HBdata.rds")
saveRDS(operator_level_entry_exit_merger_IHS,
        file = "cleaned/operator_level_entry_exit_merger_IHS.rds")