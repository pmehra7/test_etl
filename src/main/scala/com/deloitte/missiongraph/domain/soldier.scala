package com.deloitte.missiongraph.domain

import com.datastax.driver.core.DataType
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import com.deloitte.missiongraph.GraphSchema._
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
  def getSchemaColumns: List[(String,String)] = schema.toList.map(x => (x.productElement(0).toString,x.productElement(1).toString))

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
      StructField("Address_Street", StringType, nullable = true),
      StructField("Address_City", StringType, nullable = true),
      StructField("Address_State", StringType, nullable = true),
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


  def dseSchemaTransactional(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, transactionalTable)
      .ifNotExists()
      .addPartitionKey("UIC", DataType.text())
      .addClusteringColumn("INSERT_TIME", DataType.timestamp())
      .addColumn("RGMT_CD", DataType.text())
      .addColumn("CONUS_IN_CD", DataType.text())
      .addColumn("MIL_SVC_COMP_CD", DataType.text())
      .addColumn("COMPO_CD", DataType.text())
      .addColumn("MIL_PERS_CLAS_CD", DataType.text())
      .addColumn("RGMT_HOMEBASE_CD", DataType.text())
      .addColumn("AD_VOL_STAT_CD", DataType.text())
      .addColumn("AD_AVAIL_YM", DataType.text())
      .addColumn("PHOTO_DT", DataType.text())
      .addColumn("MIL_PAY_LVL_SER_NR", DataType.text())
      .addColumn("SGLI_COVG_CD", DataType.text())
      .addColumn("CTRL_GRP_CD", DataType.text())
      .addColumn("DEERS_ENRL_STAT_CD", DataType.text())
      .addColumn("DEERS_ENRL_STAT_DT", DataType.text())
      .addColumn("PROM_CONS_RCMD_CD", DataType.text())
      .addColumn("INIT_MIL_ENTRY_DT", DataType.text())
      .addColumn("MIL_EAD_DT", DataType.text())
      .addColumn("UNIT_RLSE_DT", DataType.text())
      .addColumn("UNIT_RLSE_MO_QY", DataType.text())
      .addColumn("ORDER_RSTRCTN_DT", DataType.text())
      .addColumn("ACT_FED_SVC_MO_QY", DataType.text())
      .addColumn("ACT_FED_SVC_DAY_QY", DataType.text())
      .addColumn("PRI_ACT_DISPT_CD", DataType.text())
      .addColumn("PERS_ACT_PEND_CD", DataType.text())
      .addColumn("RC_TNG_RET_GRP_CD", DataType.text())
      .addColumn("NON_MIL_EMPLR_CD", DataType.text())
      .addColumn("RC_CAT_CD", DataType.text())
      .addColumn("ARNG_PPN_CD", DataType.text())
      .addColumn("SEL_RES_TRNSTN_CD", DataType.text())
      .addColumn("PROM_SVC_ST_DT", DataType.text())
      .addColumn("AFRM_AWD_ELIG_YM", DataType.text())
      .addColumn("ARCAM_AWD_ELIG_YM", DataType.text())
      .addColumn("BASD_DT", DataType.text())
      .addColumn("BESD_DT", DataType.text())
      .addColumn("BOSD_DT", DataType.text())
      .addColumn("PEBD_DT", DataType.text())
      .addColumn("RC_GAIN_DT", DataType.text())
      .addColumn("DUAL_MPC_CD", DataType.text())
      .addColumn("DUAL_COMP_STAT_CD", DataType.text())
      .addColumn("DUAL_COMP_OER_DT", DataType.text())
      .addColumn("EVAL_PD_END_DT", DataType.text())
      .addColumn("PROM_LIST_SEQ_NR", DataType.text())
      .addColumn("PROM_LIST_DT", DataType.text())
      .addColumn("SRC_CALL_AD_UIC_CD", DataType.text())
      .addColumn("OFF_PPN_CD", DataType.text())
      .addColumn("APPT_SVC_COMP_CD", DataType.text())
      .addColumn("CMD_STAT_CD", DataType.text())
      .addColumn("SHORT_OS_TOUR_QY", DataType.text())
      .addColumn("LONG_OS_TOUR_QY", DataType.text())
      .addColumn("SEP_DELAY_RSN_CD", DataType.text())
      .addColumn("PERS_STR_STAT_CD", DataType.text())
      .addColumn("MIL_ID_CARD_NR", DataType.text())
      .addColumn("MIL_ID_CARD_TYP", DataType.text())
      .addColumn("SYS_UPDATE_USER_ID_x", DataType.text())
      .addColumn("SYS_UPDATE_DT_x", DataType.text())
      .addColumn("SGLI_ELECT_DT", DataType.text())
      .addColumn("SGLI_ENT_UBC_DT", DataType.text())
      .addColumn("PERS_DATA_VER_DT", DataType.text())
      .addColumn("MIL_PPA_CD", DataType.text())
      .addColumn("CORR_CRS_CR_HR_COMPL", DataType.text())
      .addColumn("CORR_CRS_CH_COM_DT", DataType.text())
      .addColumn("ASG_PCS_DT", DataType.text())
      .addColumn("ASG_DEROS_DT", DataType.text())
      .addColumn("ASG_DROS_DT", DataType.text())
      .addColumn("PREV_PERS_STR_STAT_CD", DataType.text())
      .addColumn("Soldier_SSN", DataType.text())
      .addColumn("EDIPI", DataType.text())
      .addColumn("DRRSA_PERS_ID2", DataType.text())
      .addColumn("FullName", DataType.text())
      .addColumn("FirstName", DataType.text())
      .addColumn("LastName", DataType.text())
      .addColumn("MiddleName", DataType.text())
      .addColumn("Address_Street", DataType.text())
      .addColumn("Address_City", DataType.text())
      .addColumn("Address_State", DataType.text())
      .addColumn("ETSDate", DataType.text())
      .addColumn("MOS", DataType.text())
      .addColumn("SecurityClearance", DataType.text())
      .addColumn("Rank", DataType.text())
      .addColumn("Grade", DataType.text())
      .addColumn("BirthCountry", DataType.text())
      .addColumn("SSN_x", DataType.text())
      .addColumn("SCTY_CLR_DT", DataType.text())
      .addColumn("SCTY_CLR_LVL_CD", DataType.text())
      .addColumn("SCTY_CLR_AUTHTY_CD", DataType.text())
      .addColumn("SYS_UPDATE_USER_ID_y", DataType.text())
      .addColumn("SYS_UPDATE_DT_y", DataType.text())
      .addColumn("SecCLearance", DataType.text())
      .addColumn("Latest_Date", DataType.text())
      .addColumn("Latest_Date_Flag", DataType.text())
      .addColumn("SecurityClass", DataType.text())
      .addColumn("SSN_y", DataType.text())
      .addColumn("BIRTH_CITY_NM", DataType.text())
      .addColumn("BIRTH_CNTY_CD", DataType.text())
      .addColumn("BIRTH_CNTRY_CD", DataType.text())
      .addColumn("SEX_CD", DataType.text())
      .addColumn("RACE_POP_CD", DataType.text())
      .addColumn("BIRTH_STATE_AB", DataType.text())
      .addColumn("ETHNIC_GRP_CD", DataType.text())
      .addColumn("CTZSP_CNTRY_CD", DataType.text())
      .addColumn("CTZSP_ORGN_CD", DataType.text())
      .addColumn("MARTL_STAT_DT", DataType.text())
      .addColumn("BIRTH_DT", DataType.text())
      .addColumn("DEPN_QY", DataType.text())
      .addColumn("MARTL_STAT_CD", DataType.text())
      .addColumn("ADULT_DEPN_QY", DataType.text())
      .addColumn("CHILD_DEPN_QY", DataType.text())
      .addColumn("REL_DENOM_CD", DataType.text())
      .addColumn("PRIOR_MIL_SVC_CD", DataType.text())
      .addColumn("SEL_SVC_CLASS_CD", DataType.text())
      .addColumn("SSN_VER_CD", DataType.text())
      .addColumn("PREV_SSN", DataType.text())
      .addColumn("BLOOD_TYP_GRP_CD", DataType.text())
      .addColumn("BLOOD_TYP_ANT_CD", DataType.text())
      .addColumn("EMAIL_ADDR_TX", DataType.text())
      .addColumn("ALT_EMAIL_ADDR_TX", DataType.text())
      .addColumn("TOTAL_ARMY_COMP_CD", DataType.text())
      .addColumn("TOTAL_ARMY_CAT_CD", DataType.text())
      .addColumn("TAPDB_REC_STAT_CD", DataType.text())
      .addColumn("TAPDB_REC_STAT_DT", DataType.text())
      .addColumn("CIV_ED_LVL_CD", DataType.text())
      .addColumn("BIRTH_PL_VAL_DT", DataType.text())
      .addColumn("CTZSP_VAL_DT", DataType.text())
      .addColumn("UNIT_LNAME", DataType.text())
      .addColumn("processed", DataType.cboolean())
      .addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }

  def dseSchemaEntity(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, entityTable)
      .ifNotExists()
      .addPartitionKey("UIC", DataType.text())
      .addColumn("RGMT_CD", DataType.text())
      .addColumn("CONUS_IN_CD", DataType.text())
      .addColumn("MIL_SVC_COMP_CD", DataType.text())
      .addColumn("COMPO_CD", DataType.text())
      .addColumn("MIL_PERS_CLAS_CD", DataType.text())
      .addColumn("RGMT_HOMEBASE_CD", DataType.text())
      .addColumn("AD_VOL_STAT_CD", DataType.text())
      .addColumn("AD_AVAIL_YM", DataType.text())
      .addColumn("PHOTO_DT", DataType.text())
      .addColumn("MIL_PAY_LVL_SER_NR", DataType.text())
      .addColumn("SGLI_COVG_CD", DataType.text())
      .addColumn("CTRL_GRP_CD", DataType.text())
      .addColumn("DEERS_ENRL_STAT_CD", DataType.text())
      .addColumn("DEERS_ENRL_STAT_DT", DataType.text())
      .addColumn("PROM_CONS_RCMD_CD", DataType.text())
      .addColumn("INIT_MIL_ENTRY_DT", DataType.text())
      .addColumn("MIL_EAD_DT", DataType.text())
      .addColumn("UNIT_RLSE_DT", DataType.text())
      .addColumn("UNIT_RLSE_MO_QY", DataType.text())
      .addColumn("ORDER_RSTRCTN_DT", DataType.text())
      .addColumn("ACT_FED_SVC_MO_QY", DataType.text())
      .addColumn("ACT_FED_SVC_DAY_QY", DataType.text())
      .addColumn("PRI_ACT_DISPT_CD", DataType.text())
      .addColumn("PERS_ACT_PEND_CD", DataType.text())
      .addColumn("RC_TNG_RET_GRP_CD", DataType.text())
      .addColumn("NON_MIL_EMPLR_CD", DataType.text())
      .addColumn("RC_CAT_CD", DataType.text())
      .addColumn("ARNG_PPN_CD", DataType.text())
      .addColumn("SEL_RES_TRNSTN_CD", DataType.text())
      .addColumn("PROM_SVC_ST_DT", DataType.text())
      .addColumn("AFRM_AWD_ELIG_YM", DataType.text())
      .addColumn("ARCAM_AWD_ELIG_YM", DataType.text())
      .addColumn("BASD_DT", DataType.text())
      .addColumn("BESD_DT", DataType.text())
      .addColumn("BOSD_DT", DataType.text())
      .addColumn("PEBD_DT", DataType.text())
      .addColumn("RC_GAIN_DT", DataType.text())
      .addColumn("DUAL_MPC_CD", DataType.text())
      .addColumn("DUAL_COMP_STAT_CD", DataType.text())
      .addColumn("DUAL_COMP_OER_DT", DataType.text())
      .addColumn("EVAL_PD_END_DT", DataType.text())
      .addColumn("PROM_LIST_SEQ_NR", DataType.text())
      .addColumn("PROM_LIST_DT", DataType.text())
      .addColumn("SRC_CALL_AD_UIC_CD", DataType.text())
      .addColumn("OFF_PPN_CD", DataType.text())
      .addColumn("APPT_SVC_COMP_CD", DataType.text())
      .addColumn("CMD_STAT_CD", DataType.text())
      .addColumn("SHORT_OS_TOUR_QY", DataType.text())
      .addColumn("LONG_OS_TOUR_QY", DataType.text())
      .addColumn("SEP_DELAY_RSN_CD", DataType.text())
      .addColumn("PERS_STR_STAT_CD", DataType.text())
      .addColumn("MIL_ID_CARD_NR", DataType.text())
      .addColumn("MIL_ID_CARD_TYP", DataType.text())
      .addColumn("SYS_UPDATE_USER_ID_x", DataType.text())
      .addColumn("SYS_UPDATE_DT_x", DataType.text())
      .addColumn("SGLI_ELECT_DT", DataType.text())
      .addColumn("SGLI_ENT_UBC_DT", DataType.text())
      .addColumn("PERS_DATA_VER_DT", DataType.text())
      .addColumn("MIL_PPA_CD", DataType.text())
      .addColumn("CORR_CRS_CR_HR_COMPL", DataType.text())
      .addColumn("CORR_CRS_CH_COM_DT", DataType.text())
      .addColumn("ASG_PCS_DT", DataType.text())
      .addColumn("ASG_DEROS_DT", DataType.text())
      .addColumn("ASG_DROS_DT", DataType.text())
      .addColumn("PREV_PERS_STR_STAT_CD", DataType.text())
      .addColumn("Soldier_SSN", DataType.text())
      .addColumn("EDIPI", DataType.text())
      .addColumn("DRRSA_PERS_ID2", DataType.text())
      .addColumn("FullName", DataType.text())
      .addColumn("FirstName", DataType.text())
      .addColumn("LastName", DataType.text())
      .addColumn("MiddleName", DataType.text())
      .addColumn("Address_Street", DataType.text())
      .addColumn("Address_City", DataType.text())
      .addColumn("Address_State", DataType.text())
      .addColumn("ETSDate", DataType.text())
      .addColumn("MOS", DataType.text())
      .addColumn("SecurityClearance", DataType.text())
      .addColumn("Rank", DataType.text())
      .addColumn("Grade", DataType.text())
      .addColumn("BirthCountry", DataType.text())
      .addColumn("SSN_x", DataType.text())
      .addColumn("SCTY_CLR_DT", DataType.text())
      .addColumn("SCTY_CLR_LVL_CD", DataType.text())
      .addColumn("SCTY_CLR_AUTHTY_CD", DataType.text())
      .addColumn("SYS_UPDATE_USER_ID_y", DataType.text())
      .addColumn("SYS_UPDATE_DT_y", DataType.text())
      .addColumn("SecCLearance", DataType.text())
      .addColumn("Latest_Date", DataType.text())
      .addColumn("Latest_Date_Flag", DataType.text())
      .addColumn("SecurityClass", DataType.text())
      .addColumn("SSN_y", DataType.text())
      .addColumn("BIRTH_CITY_NM", DataType.text())
      .addColumn("BIRTH_CNTY_CD", DataType.text())
      .addColumn("BIRTH_CNTRY_CD", DataType.text())
      .addColumn("SEX_CD", DataType.text())
      .addColumn("RACE_POP_CD", DataType.text())
      .addColumn("BIRTH_STATE_AB", DataType.text())
      .addColumn("ETHNIC_GRP_CD", DataType.text())
      .addColumn("CTZSP_CNTRY_CD", DataType.text())
      .addColumn("CTZSP_ORGN_CD", DataType.text())
      .addColumn("MARTL_STAT_DT", DataType.text())
      .addColumn("BIRTH_DT", DataType.text())
      .addColumn("DEPN_QY", DataType.text())
      .addColumn("MARTL_STAT_CD", DataType.text())
      .addColumn("ADULT_DEPN_QY", DataType.text())
      .addColumn("CHILD_DEPN_QY", DataType.text())
      .addColumn("REL_DENOM_CD", DataType.text())
      .addColumn("PRIOR_MIL_SVC_CD", DataType.text())
      .addColumn("SEL_SVC_CLASS_CD", DataType.text())
      .addColumn("SSN_VER_CD", DataType.text())
      .addColumn("PREV_SSN", DataType.text())
      .addColumn("BLOOD_TYP_GRP_CD", DataType.text())
      .addColumn("BLOOD_TYP_ANT_CD", DataType.text())
      .addColumn("EMAIL_ADDR_TX", DataType.text())
      .addColumn("ALT_EMAIL_ADDR_TX", DataType.text())
      .addColumn("TOTAL_ARMY_COMP_CD", DataType.text())
      .addColumn("TOTAL_ARMY_CAT_CD", DataType.text())
      .addColumn("TAPDB_REC_STAT_CD", DataType.text())
      .addColumn("TAPDB_REC_STAT_DT", DataType.text())
      .addColumn("CIV_ED_LVL_CD", DataType.text())
      .addColumn("BIRTH_PL_VAL_DT", DataType.text())
      .addColumn("CTZSP_VAL_DT", DataType.text())
      .addColumn("UNIT_LNAME", DataType.text())
      //.addColumn("processed", DataType.cboolean())
      //.addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }

  private val partitionProperties = List(
    property("UIC",GraphDataType.Text,Cardinality.single)
  )

  private val clusteringProperties = List(
    property("INSERT_TIME",GraphDataType.Text,Cardinality.single)
  )

  private val properties = List(
    property("UNIT_LNAME",GraphDataType.Text,Cardinality.single),
    property("UNIT_HOME_FORT_NAME",GraphDataType.Text,Cardinality.single),
    property("UNIT_LOCATION_ZIP_CODE",GraphDataType.Text,Cardinality.single),
    property("UNIT_GEOCODE",GraphDataType.Text,Cardinality.single),
    property("EquipUIC",GraphDataType.Text,Cardinality.single)
  )

  private val allProperties = partitionProperties ::: clusteringProperties ::: properties

  private val unitVertex = vertex("soldier", properties, partitionProperties, clusteringProperties)

  def getGraphSchema: Map[String, List[String]] = Map(
    GraphObject.Vertex.toString -> List(createVertexSchema(unitVertex)),
    GraphObject.Property.toString -> createPropertySchema(allProperties)
  )

}













