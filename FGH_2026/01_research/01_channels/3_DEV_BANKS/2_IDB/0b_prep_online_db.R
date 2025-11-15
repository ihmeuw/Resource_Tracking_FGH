#
# prep the online IDB disbursement data so that it is ready to be used by the 
# mighty stata script 1_CREATE_IDB.do
# (the stata script uses this data, but was written for a slightly older version
# and hence we can reformat the data here to make things easier)


code_repo <- 'FILEPATH'

report_year <- 2024

source(paste0(code_repo, "/FGH_", report_year, "/utils.R"))

pacman::p_load(
    openxlsx,
    readxl
)


d <- readxl::read_excel(get_path("idb", "raw",
                                 "All_IDB_Health_projects_20250305.xlsx")) 
setDT(d)

names(d) <- gsub(" ", "", names(d), fixed = TRUE)
names(d) <- gsub("_", "", names(d), fixed = TRUE)

setnames(d,
         c("ProjectNumber", "ProjectName", "ApprovalAmount",
           "ProjectDescription", "ProjectType", "Sector"),
         c("Projectnumber", "Projecttitle", "ApprovalAmountUSM",
           "Projectdescription", "ProjectOperationtype", "ProjectSector"))


d <- d[!is.na(Projectnumber) & !is.na(ApprovalDate)]
d[, ApprovalDate := as.character(as.Date(ApprovalDate, format = "%Y-%m-%d"))]


# need to isolate unique project numbers
idcols <- c(
    grep("^Project", names(d), value = TRUE),
    "Status", "ESGClassification"
)
du <- d[, .(ApprovalAmountUSM = sum(ApprovalAmountUSM, na.rm = TRUE),
            ApprovalDate = min(ApprovalDate, na.rm = TRUE)),
        by = idcols]

if (nrow(du[duplicated(Projectnumber)])) {
    stop("Duplicate project numbers")
}




# stata expects an excel file...
openxlsx::write.xlsx(du, get_path("idb", "raw", "all_idb_clean.xlsx"),
                     rowNames = FALSE)
