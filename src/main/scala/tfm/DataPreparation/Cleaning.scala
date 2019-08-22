package tfm.DataPreparation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import tfm.NLPProcessor

class Cleaning {
  def clean(df: DataFrame): DataFrame ={
    val column = "Queries"
    val dfApostropheCleaned = apostropheCleaning(df, column )

    ???
  }

  /**
    * Eliminación de apostrofe. I'd like to ... -->  I would like to
    * @param df
    * @param column
    * @return
    */
  def apostropheCleaning(df:DataFrame, column: String): DataFrame={
    val s = "I'd like to ride a bike"

    val processor = new NLPProcessor(s)
    val tags = processor.getPosTags
    val tokens = processor.getTokens
    val pos_tags = for (index <- 0 to tags.length) yield  (tags(index),tokens(index))



    val nlengthCleanUDF = udf { s: String =>
      val processor = new NLPProcessor(s)
      val tags = processor.getPosTags
      val tokens = processor.getTokens
      val pos_tags = for (index <- 0 to tags.length) yield  (tags(index),tokens(index))
      pos_tags


      //manage_ambiguos_apostrophes()


    }


    df

    ???
  }

  def manage_ambiguos_apostrophes(first: (String, String), second: (String, String)) : String = {
    if (first._1 == "'s") {
      if (second._2 == "VBN")
        return "has"
      else if (first._2 != "POS")
        return "is"
    }
    else if (first._1 == "'d")
    {
      if (second._2 == "VBN")
        return "had"
      else
        return "would"
    }
    else if (second._1.contains("'") && second._1 != "'s" && second._1 != "'d")

      return first._1 + second._1
    else if (!first._1.contains("'") )
      return first._1
    return ""

  }



  /**
    * Separación de palabras con mayúsculas. GoodBye --> Good Bye
    */
  def attachedWordsCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }


  /**
    * Eliminación de caracteres definidos por usuario como ç o ñ. çhola --> hola
    */
  def charactersCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }

  /**
    * Eliminación de palabras definidas por usuario.
    */
  def contentCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }

  /**
    * Decodificación Caracteres
    */
  def decodingCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }

  /**
    * Eliminacion de expresiones. Eliminar "good bye". good bye my friend --> my friend
    */
  def expressionsCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }

  /**
    * Corrección gramática. "she love him" -->  "She loves him"
    */
  def grammarCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }

  /**
    * Extración de text de HTML. "<html>&bnsp;hola</html>" --> "hola"
    */
  def htmlCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }


  /**
    * Eliminacion de comillas en el primer y ultimo carácter. 'Hola' --> Hola
    */
  def initialFinalApostropheCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }

  /**
    * Traducción de siglas o abreviaturas. "AFAIK" --> "As Far As I Know"
    */
  def slangCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }

  /**
    *
    */
  def standarizingCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }

  /**
    * --> Limpieza de URL. www.google.com --> Google, www.google.com --> [None]
    */
  def urlCleaning(df:DataFrame, column: String): DataFrame={
    ???
  }
}
