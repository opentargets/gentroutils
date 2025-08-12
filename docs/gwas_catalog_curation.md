README

How to do the manual curation?

1. Make the gsutil_list of susmtats from google bucket:

gsutil -u open-targets-genetics-dev ls 'gs://gwas_catalog_inputs/raw_summary_statistics/**/*h.tsv.gz' > gsutil_list.txt

2. Download latest "All studies v1.0.3.1 - with added fields to match the unpublished downloads, including cohort identifiers and full summary statistics availability" from

https://www.ebi.ac.uk/gwas/docs/file-downloads

3. Put the ouput from the latest curation into your working folder, e.g.:

20241219_output_curation.txt

4. Execute 00_prepare_tables_for_curation.R (you need to open the script and change file names etc and execute it line by line). It should generate the table -

20250426_input_for_curation.tsv

5. Convert it to EXCEL format for convinence

6. Make the curartion:

-For each study with isCurated==FALSE check the title/abstract/publication text

-Check whether stydyType (studyType) is "pQTL" or "no_licence" or any other molQTL. Put it into stydyType if needed. Leave empty otherwise.

-Check whether the anlysis design (anlysisFlag) is:

1. Case-case study
2. ExWAS
3. GxE
4. GxG
5. Metabolite
6. Multivariate analysis
7. Non-additive model
8. wgsGWAS

Put it into anlysisFlag if needed. Leave empty otherwise. Don't wory about making mistakes. The curation is not ideal.

-Do it for all rows with isCurated==FALSE. When curation is finsihed don't forget to put there TRUE.
In the very end all rows in the isCurated should be "TRUE".

7. Save it in tab delimited format as ouput.
