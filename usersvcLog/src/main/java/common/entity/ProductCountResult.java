package common.entity;

import com.fasterxml.jackson.annotation.JsonGetter;

public class ProductCountResult {

    private String yearMonth;
    private String applicationId;
    private String productName;
    private String productGroup;
    private int activeDeviceCount;

    public ProductCountResult(String yearMonth, String applicationId, String productName, String productGroup, int activeDeviceCount) {
        this.yearMonth = yearMonth;
        this.applicationId = applicationId;
        this.productName = productName;
        this.productGroup = productGroup;
        this.activeDeviceCount = activeDeviceCount;
    }

    @JsonGetter("report_month")
    public String getYearMonth() {
        return yearMonth;
    }

    @JsonGetter("app_id")
    public String getApplicationId() {
        return applicationId;
    }

    @JsonGetter("product_name")
    public String getProductName() {
        return productName;
    }

    @JsonGetter("product_group")
    public String getProductGroup() {
        return productGroup;
    }

    @JsonGetter("device_count")
    public int getActiveDeviceCount() {
        return activeDeviceCount;
    }


}
