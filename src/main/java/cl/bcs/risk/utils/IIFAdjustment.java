package cl.bcs.risk.utils;

import cl.bcs.risk.pipeline.Record;


/**
 * @author Alberto Hormazabal Cespedes
 * @author exaTech Ingenieria SpA. (info@exatech.cl)
 */
public enum IIFAdjustment {

  /** Pesos nominales, pagadero en pesos */
  CLP_CLP("N", "Pesos nominales, pagadero en pesos"),

  /** Reajustabilidad en Unidades de Fomento, pagadero en pesos. */
  CLF_CLP("U", "Reajustabilidad en Unidades de Fomento, pagadero en pesos."),

  /** Reajustabilidad en dólares americanos, pagadero en pesos. */
  USD_CLP("D", "Reajustabilidad en dólares americanos, pagadero en pesos."),

  /** Reajustabilidad en dólares americanos, pagadero en dólares. */
  USD_USD("*", "Reajustabilidad en dólares americanos, pagadero en dólares."),

  /** Reajustabilidad en Euros, pagadero en pesos. */
  EUR_CLP("E", "Reajustabilidad en Euros, pagadero en pesos."),

  /** Reajustabilidad en Euros, pagadero en Euros. */
  EUR_EUR("R", "Reajustabilidad en Euros, pagadero en Euros."),

  /** Reajustabilidad en Indice Valor Promedio, pagadero en pesos */
  IVP_CLP("I", "Reajustabilidad en Indice Valor Promedio, pagadero en pesos"),

  /** Otro tipo de reajustabilidad */
  OTHER("O", "Otro tipo de reajustabilidad");

  private String code;
  private String description;

  IIFAdjustment(String code, String description) {
    this.code = code;
    this.description = description;
  }

  public String getCode() {
    return code;
  }

  public String getDescription() {
    return description;
  }


  /**
   * Obtiene el tipo de reajuste para el instrumento representado en el registro
   * y emisor entregados.
   *
   * @param i Emisor del instrumento
   * @param r Registro con instrumento. Debe tener un campo "moneda".
   * @return Tipo de reajuste IIF.
   */
  public static IIFAdjustment getAdjustment(String currency, IIFSymbolUtils.Issuer i) {

    switch (currency) {
      case "CLP":
        return CLP_CLP;

      case "CLF":
        return CLF_CLP;

      case "USD":
        return USD_CLP;

      case "EUR":
        return EUR_CLP;

      case "IVP":
        return IVP_CLP;

      default:
        return OTHER;
    }
  }
}
