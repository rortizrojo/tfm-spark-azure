package tfm.DataPreparation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_replace, udf}
import tfm.NLPProcesor

class Cleaning {
  def clean(df: DataFrame): DataFrame ={
    val column = "Queries"
    val dfApostropheCleaned = apostropheCleaning(df, column )

    dfApostropheCleaned
  }

  /**
    * Eliminación de apostrofe. I'd like to ... -->  I would like to
    * @param df
    * @param column
    * @return
    */
  def apostropheCleaning(df:DataFrame, column: String): DataFrame={
    val model = new NLPProcesor(column)
    model.transform(df)
  }



  /**
    * Separación de palabras con mayúsculas. GoodBye --> Good Bye
    */
  def attachedWordsCleaning(df:DataFrame, column: String): DataFrame={
    import scala.util.matching.Regex
    val regexCamelCase = "[A-Z][^A-Z]+"
    val keyValPattern:Regex = regexCamelCase.r

    val attachedWordsCleaningUDF = udf { s: String =>
      s.split(" ").map(
        x =>
          {
            if(keyValPattern.findAllMatchIn(x).isEmpty)
              x
            else
              keyValPattern.findAllMatchIn(x).mkString(" ")
          }).mkString(" ")
    }
    df.withColumn(column,attachedWordsCleaningUDF(col(column)))
  }


  /**
    * Eliminación de caracteres definidos por usuario como ç o ñ. çhola --> hola
    */
  def charactersCleaning(df:DataFrame, column: String, charList: List[Char]): DataFrame={
    /** FoldLeft sirve para aplicar una operación a los elementos de una colección dando el valor inicial que se indique.
      * En este caso para la lista de caracteres List(ç,ñ) lo que hace es poner el paréntesis al principio y luego aplica
      * la operación "+" a todos los elementos, es decir concatenar los caracteres de la lista quedando "(çñ" una vez hecha
      * la operación se le añade + ")" para que quede la expresión regular (ç|ñ) que servirá para eliminar estos caracteres
      */
    val regExpIntern = charList.foldLeft("")(_ + '|' +  _)
    val regExp = "(" + regExpIntern.subSequence(1,regExpIntern.length ) + ")"
    df.withColumn(column, regexp_replace(col(column), regExp, ""))
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
