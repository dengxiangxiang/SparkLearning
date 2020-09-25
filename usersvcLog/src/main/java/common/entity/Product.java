package common.entity;

public class Product {
    private String productName;
    private String productGroup;

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductGroup() {
        return productGroup;
    }

    public void setProductGroup(String productGroup) {
        this.productGroup = productGroup;
    }

    public Product(String productName, String productGroup){
        this.productName = productName;
        this.productGroup = productGroup;
    }


}
