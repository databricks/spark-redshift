package com.databricks.spark.redshift

import java.util
import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec

import com.amazonaws.services.s3.model.{EncryptionMaterials, EncryptionMaterialsProvider}
import com.amazonaws.util.Base64
import org.apache.hadoop.conf.{Configurable, Configuration}


/**
  * This class provides encryption materials to be used by spark-redshfit
  * library. It gets the master symmetric key from hadoop configuration,
  * converts it into EncryptionMaterials and returns them.
  */
object RedshiftEncryptionMaterialsProvider {
    /**
      * Encryption algorithm used for by Redshift.
      */
    private val ENCRYPTION_ALGORITHM: String = "AES"
    /**
      * This is the hadoop configuration property name that contains
      * redshift master symmetric key.
      */
    private val SPARK_REDSHIFT_MASTER_SYM_KEY: String = "spark-redshift.master-sym-key"
}

class RedshiftEncryptionMaterialsProvider
    extends EncryptionMaterialsProvider
    with Configurable {
    /**
      * The hadoop configuration.
      */
    private var conf: Configuration = null

    def setConf(conf: Configuration) {
        this.conf = conf
    }

    def getConf: Configuration = {
        return this.conf
    }

    def refresh {
    }

    def getEncryptionMaterials(materialsDescription: Map[String, String]):
    EncryptionMaterials = {
        return getEncryptionMaterials
    }

    override def getEncryptionMaterials(
        materialsDescription: util.Map[String, String]):
    EncryptionMaterials = getEncryptionMaterials

    override def getEncryptionMaterials: EncryptionMaterials = {
        val masterKey: SecretKey =
            new SecretKeySpec(
                Base64.decode(
                    conf.get(
                        RedshiftEncryptionMaterialsProvider.
                            SPARK_REDSHIFT_MASTER_SYM_KEY)),
                RedshiftEncryptionMaterialsProvider.ENCRYPTION_ALGORITHM)
        return new EncryptionMaterials(masterKey)
    }
}
