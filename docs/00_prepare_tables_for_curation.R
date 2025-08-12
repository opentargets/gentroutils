library(data.table)
library(magrittr)
library(dplyr)
library(stringr)
system(
  "gcloud storage ls 'gs://gwas_catalog_inputs/raw_summary_statistics/**/*.h.tsv.gz' > gsutil_list.txt"
)
system(
  "gcloud storage ls 'gs://gwas_catalog_inputs/harmonised_summary_statistics/' > gsutil_list_harmonised.txt"
)
system(
  "gcloud storage cp gs://gwas_catalog_inputs/gentroutils/latest/gwas_catalog_download_studies.tsv gwas_catalog_download_studies.tsv"
)
system(
  "gcloud storage cp gs://gwas_catalog_inputs/curation/202507/GWAS_Catalog_study_curation.tsv prev_curation.tsv"
)

harmonised_by_gwas_catalog <- fread(
  "gwas_catalog_download_studies.tsv",
  data.table = FALSE,
  quote = ""
) %>%
  tibble::as_tibble() %>%
  dplyr::select(
    studyId = `STUDY ACCESSION`,
    pubmedId = `PUBMED ID`,
    hasSumstats = `FULL SUMMARY STATISTICS`
  ) %>%
  dplyr::mutate(
    hasSumstats = dplyr::case_when(
      hasSumstats == "Yes" ~ TRUE,
      hasSumstats == "No" ~ FALSE,
      hasSumstats == "yes" ~ TRUE,
      hasSumstats == "no" ~ FALSE,
      hasSumstats == "Unknown" ~ NA,
      TRUE ~ NA
    )
  ) %>%
  dplyr::filter(!is.na(hasSumstats))

harmonised_by_ot <- fread(
  "gsutil_list_harmonised.txt",
  data.table = FALSE,
  header = FALSE
) %>%
  tibble::as_tibble() %>%
  dplyr::rename(harmonisedSumstatsPath = V1) %>%
  dplyr::filter(harmonisedSumstatsPath != "") %>%
  dplyr::filter(!endsWith(harmonisedSumstatsPath, "statistics/")) %>%
  dplyr::filter(!endsWith(harmonisedSumstatsPath, "statistics/:")) %>%
  dplyr::mutate(
    studyId = stringr::str_extract(
      harmonisedSumstatsPath,
      ".*(GCST\\d+).*",
      group = 1
    )
  ) %>%
  dplyr::mutate(isHarmonisedByOt = TRUE) %>%
  dplyr::select(studyId, harmonisedSumstatsPath, isHarmonisedByOt)

synced_to_gcs <- fread(
  "gsutil_list.txt",
  data.table = FALSE,
  header = FALSE
) %>%
  tibble::as_tibble() %>%
  dplyr::rename(sumstatsPath = V1) %>%
  dplyr::mutate(
    studyId = stringr::str_extract(sumstatsPath, ".*(GCST\\d+).*", group = 1)
  ) %>%
  dplyr::select(studyId, sumstatsPath)

previous_curation <- fread(
  "prev_curation.tsv",
  data.table = FALSE,
  header = TRUE
) %>%
  tibble::as_tibble()

new_studies_harmonised_by_gwas_catalog <- harmonised_by_gwas_catalog %>%
  dplyr::filter(hasSumstats) %>%
  dplyr::anti_join(previous_curation, by = "studyId") %>%
  dplyr::select(studyId, pubmedId, hasSumstats)

new_studies_harmonised_by_gwas_catalog_and_synced <- new_studies_harmonised_by_gwas_catalog %>%
  dplyr::inner_join(synced_to_gcs, by = "studyId") %>%
  dplyr::select(studyId, pubmedId, sumstatsPath) %>%
  dplyr::mutate(
    isCurated = FALSE,
    studyType = "",
    analysisFlag = "",
    qualityControl = NA_character_,
  ) %>%
  dplyr::select(
    studyId,
    studyType,
    analysisFlag,
    qualityControl,
    isCurated,
    pubmedId
  )

new_curation <- new_studies_harmonised_by_gwas_catalog_and_synced %>%
  dplyr::select(studyId) %>%
  dplyr::full_join(previous_curation, by = "studyId") %>%
  dplyr::select(studyId, studyType = studyType, analysisFlag = analysisFlag, qualityControl = qualityControl, isCurated = isCurated, pubmedId = pubmedId)

removed_studies_by_gwas_catalog <- previous_curation %>%
  dplyr::anti_join(synced_to_gcs, by = "studyId") %>%
  dplyr::left_join(harmonised_by_ot, by = "studyId") %>%
  dplyr::mutate(isHarmonisedByOt = dplyr::coalesce(isHarmonisedByOt, FALSE)) %>%
  dplyr::select(studyId, pubmedId, isHarmonisedByOt) %>%
  dplyr::group_by(pubmedId, isHarmonisedByOt) %>%
  dplyr::summarise(n = n(), .groups = )

n_new_rows <- new_curation %>%
  dplyr::anti_join(previous_curation, by = "studyId") %>%
  nrow()


fwrite(
  x = new_curation,
  file = "20250808_input_for_curation.tsv",
  sep = "\t",
  quote = FALSE
)
