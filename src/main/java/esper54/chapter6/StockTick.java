package esper54.chapter6;

/**
 * Created by liwangchun on 16/10/31.
 */
public class StockTick {
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    private String symbol;

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    private double price;
}
