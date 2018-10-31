package com.deloitte.missiongraph.domain

import com.deloitte.missiongraph.sourcedata.emilpo
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class soldier extends emilpo {

  var transactionalTable: String = "transactional_soldier"
  def setTransactionalTableName(tableName: String): Unit = this.transactionalTable = tableName
  var entityTable: String = "entity_soldier"
  def setEntityTableName(tableName: String): Unit = this.entityTable = tableName
  var delimiter = "|"
  def setDelimiter(delimiter: String): Unit = this.delimiter = delimiter
  def setFileLocation(fileLocation: String): Unit = this.fileLocation = fileLocation

  def schema:StructType = {
    StructType(Array(
      StructField("RGMT_CD", StringType, nullable = true),
      StructField("CONUS_IN_CD", StringType, nullable = true),
      StructField("MIL_SVC_COMP_CD", StringType, nullable = true),
      StructField("COMPO_CD", StringType, nullable = true),
      StructField("MIL_PERS_CLAS_CD", StringType, nullable = true),
      StructField("UIC", StringType, nullable = false),
      StructField("RGMT_HOMEBASE_CD", StringType, nullable = true),
      StructField("AD_VOL_STAT_CD", StringType, nullable = true),
      StructField("AD_AVAIL_YM", StringType, nullable = true),
      StructField("PHOTO_DT", StringType, nullable = true),
      StructField("MIL_PAY_LVL_SER_NR", StringType, nullable = true),
      StructField("SGLI_COVG_CD", StringType, nullable = true),
      StructField("CTRL_GRP_CD", StringType, nullable = true),
      StructField("DEERS_ENRL_STAT_CD", StringType, nullable = true),
      StructField("DEERS_ENRL_STAT_DT", StringType, nullable = true),
      StructField("PROM_CONS_RCMD_CD", StringType, nullable = true),
      StructField("INIT_MIL_ENTRY_DT", StringType, nullable = true),
      StructField("MIL_EAD_DT", StringType, nullable = true),
      StructField("UNIT_RLSE_DT", StringType, nullable = true),
      StructField("UNIT_RLSE_MO_QY", StringType, nullable = true),
      StructField("ORDER_RSTRCTN_DT", StringType, nullable = true),
      StructField("ACT_FED_SVC_MO_QY", StringType, nullable = true),
      StructField("ACT_FED_SVC_DAY_QY", StringType, nullable = true),
      StructField("PRI_ACT_DISPT_CD", StringType, nullable = true),
      StructField("PERS_ACT_PEND_CD", StringType, nullable = true),
      StructField("RC_TNG_RET_GRP_CD", StringType, nullable = true),
      StructField("NON_MIL_EMPLR_CD", StringType, nullable = true),
      StructField("RC_CAT_CD", StringType, nullable = true),
      StructField("ARNG_PPN_CD", StringType, nullable = true),
      StructField("SEL_RES_TRNSTN_CD", StringType, nullable = true),
      StructField("PROM_SVC_ST_DT", StringType, nullable = true),
      StructField("AFRM_AWD_ELIG_YM", StringType, nullable = true),
      StructField("ARCAM_AWD_ELIG_YM", StringType, nullable = true),
      StructField("BASD_DT", StringType, nullable = true),
      StructField("BESD_DT", StringType, nullable = true),
      StructField("BOSD_DT", StringType, nullable = true),
      StructField("PEBD_DT", StringType, nullable = true),
      StructField("RC_GAIN_DT", StringType, nullable = true),
      StructField("DUAL_MPC_CD", StringType, nullable = true),
      StructField("DUAL_COMP_STAT_CD", StringType, nullable = true),
      StructField("DUAL_COMP_OER_DT", StringType, nullable = true),
      StructField("EVAL_PD_END_DT", StringType, nullable = true),
      StructField("PROM_LIST_SEQ_NR", StringType, nullable = true),
      StructField("PROM_LIST_DT", StringType, nullable = true),
      StructField("SRC_CALL_AD_UIC_CD", StringType, nullable = true),
      StructField("OFF_PPN_CD", StringType, nullable = true),
      StructField("APPT_SVC_COMP_CD", StringType, nullable = true),
      StructField("CMD_STAT_CD", StringType, nullable = true),
      StructField("SHORT_OS_TOUR_QY", StringType, nullable = true),
      StructField("LONG_OS_TOUR_QY", StringType, nullable = true),
      StructField("SEP_DELAY_RSN_CD", StringType, nullable = true),
      StructField("PERS_STR_STAT_CD", StringType, nullable = true),
      StructField("MIL_ID_CARD_NR", StringType, nullable = true),
      StructField("MIL_ID_CARD_TYP", StringType, nullable = true),
      StructField("SYS_UPDATE_USER_ID_x", StringType, nullable = true),
      StructField("SYS_UPDATE_DT_x", StringType, nullable = true),
      StructField("SGLI_ELECT_DT", StringType, nullable = true),
      StructField("SGLI_ENT_UBC_DT", StringType, nullable = true),
      StructField("PERS_DATA_VER_DT", StringType, nullable = true),
      StructField("MIL_PPA_CD", StringType, nullable = true),
      StructField("CORR_CRS_CR_HR_COMPL", StringType, nullable = true),
      StructField("CORR_CRS_CH_COM_DT", StringType, nullable = true),
      StructField("ASG_PCS_DT", StringType, nullable = true),
      StructField("ASG_DEROS_DT", StringType, nullable = true),
      StructField("ASG_DROS_DT", StringType, nullable = true),
      StructField("PREV_PERS_STR_STAT_CD", StringType, nullable = true),
      StructField("Soldier_SSN", StringType, nullable = true),
      StructField("EDIPI", StringType, nullable = true),
      StructField("DRRSA_PERS_ID2", StringType, nullable = true),
      StructField("FullName", StringType, nullable = true),
      StructField("FirstName", StringType, nullable = true),
      StructField("LastName", StringType, nullable = true),
      StructField("MiddleName", StringType, nullable = true),
      StructField("Address-Street", StringType, nullable = true),
      StructField("Address-City", StringType, nullable = true),
      StructField("Address-State", StringType, nullable = true),
      StructField("ETSDate", StringType, nullable = true),
      StructField("MOS", StringType, nullable = true),
      StructField("SecurityClearance", StringType, nullable = true),
      StructField("Rank", StringType, nullable = true),
      StructField("Grade", StringType, nullable = true),
      StructField("BirthCountry", StringType, nullable = true),
      StructField("SSN_x", StringType, nullable = true),
      StructField("SCTY_CLR_DT", StringType, nullable = true),
      StructField("SCTY_CLR_LVL_CD", StringType, nullable = true),
      StructField("SCTY_CLR_AUTHTY_CD", StringType, nullable = true),
      StructField("SYS_UPDATE_USER_ID_y", StringType, nullable = true),
      StructField("SYS_UPDATE_DT_y", StringType, nullable = true),
      StructField("SecCLearance", StringType, nullable = true),
      StructField("Latest_Date", StringType, nullable = true),
      StructField("Latest_Date_Flag", StringType, nullable = true),
      StructField("SecurityClass", StringType, nullable = true),
      StructField("SSN_y", StringType, nullable = true),
      StructField("BIRTH_CITY_NM", StringType, nullable = true),
      StructField("BIRTH_CNTY_CD", StringType, nullable = true),
      StructField("BIRTH_CNTRY_CD", StringType, nullable = true),
      StructField("SEX_CD", StringType, nullable = true),
      StructField("RACE_POP_CD", StringType, nullable = true),
      StructField("BIRTH_STATE_AB", StringType, nullable = true),
      StructField("ETHNIC_GRP_CD", StringType, nullable = true),
      StructField("CTZSP_CNTRY_CD", StringType, nullable = true),
      StructField("CTZSP_ORGN_CD", StringType, nullable = true),
      StructField("MARTL_STAT_DT", StringType, nullable = true),
      StructField("BIRTH_DT", StringType, nullable = true),
      StructField("DEPN_QY", StringType, nullable = true),
      StructField("MARTL_STAT_CD", StringType, nullable = true),
      StructField("ADULT_DEPN_QY", StringType, nullable = true),
      StructField("CHILD_DEPN_QY", StringType, nullable = true),
      StructField("REL_DENOM_CD", StringType, nullable = true),
      StructField("PRIOR_MIL_SVC_CD", StringType, nullable = true),
      StructField("SEL_SVC_CLASS_CD", StringType, nullable = true),
      StructField("SSN_VER_CD", StringType, nullable = true),
      StructField("PREV_SSN", StringType, nullable = true),
      StructField("BLOOD_TYP_GRP_CD", StringType, nullable = true),
      StructField("BLOOD_TYP_ANT_CD", StringType, nullable = true),
      StructField("EMAIL_ADDR_TX", StringType, nullable = true),
      StructField("ALT_EMAIL_ADDR_TX", StringType, nullable = true),
      StructField("TOTAL_ARMY_COMP_CD", StringType, nullable = true),
      StructField("TOTAL_ARMY_CAT_CD", StringType, nullable = true),
      StructField("TAPDB_REC_STAT_CD", StringType, nullable = true),
      StructField("TAPDB_REC_STAT_DT", StringType, nullable = true),
      StructField("CIV_ED_LVL_CD", StringType, nullable = true),
      StructField("BIRTH_PL_VAL_DT", StringType, nullable = true),
      StructField("CTZSP_VAL_DT", StringType, nullable = true),
      StructField("UNIT_LNAME", StringType, nullable = true)
    ))
  }
}













