if (!require("data.table")) install.packages("data.table")
if (!require("dplyr")) install.packages("dplyr")

library(data.table)
library(dplyr)
library(stringr)


all_variants_files <- list()

for (FILE in list.files(
                        path = "2024-09-30_ESRP1_and_ESRP2",
                        pattern = ".tsv.gz", full.names = TRUE)){
  variants_file <- fread(FILE, header = TRUE)
  all_variants_files <- append(all_variants_files, list(variants_file))
}

combined_variants_files <- rbindlist(all_variants_files, fill = TRUE)
combined_variants_files$chromosome <-
  factor(combined_variants_files$chromosome,
         levels = c(as.character(1:22), "X", "Y"), ordered = TRUE)

combined_variants_files <- combined_variants_files %>%
  arrange(chromosome, start, study_short_name, family_id, is_proband)

# View the combined data frame
write.table(combined_variants_files,
            file = paste0("MMC_all_families_variants_", Sys.Date(), ".tsv"),
            row.names = FALSE, sep = "\t", quote = FALSE, na = "-")