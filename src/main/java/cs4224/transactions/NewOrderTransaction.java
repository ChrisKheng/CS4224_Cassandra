package cs4224.transactions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.session.Session;


import java.math.BigDecimal;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.HashSet;

public class NewOrderTransaction extends BaseTransaction {

    private Session session;
    private PreparedStatement selectwhse;
    private PreparedStatement selectdistrict;
    private PreparedStatement selectcustomer;
    private PreparedStatement selectstock;
    private PreparedStatement selectitem;

    private PreparedStatement updatedistrictnextorderID;
    private PreparedStatement updateCustomerOrder;
    private  PreparedStatement updateStock;

    private PreparedStatement insertOrderLine;
    private PreparedStatement insertOrderById;

   /** private int customerId;
    private int warehouseId;
    private int districtId;
    private int noOfItems;**/

   NewOrderTransaction(CqlSession session)
   {
       super(session);

       this.session = session;
       this.selectwhse = session.prepare("SELECT W_TAX FROM wholesale_dev_a.warehouse WHERE W_ID = ?");

   }

    private static final Format formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


   /*** public NewOrderTransaction(CqlSession session) {
        super(session);
    }***/

    @Override
    public int getExtraLines() {
        return noOfItems;
    }

    @Override
    public void execute(String[] dataLines, String[] parameters) {
        customerId = Integer.parseInt(parameters[1]);
        warehouseId = Integer.parseInt(parameters[2]);
        districtId = Integer.parseInt(parameters[3]);
        noOfItems = Integer.parseInt(parameters[4]);

        List<Integer> itemnos = new ArrayList<>();
        List<Integer> supplierwhse = new ArrayList<>();
        List<Integer> qty = new ArrayList<>();

        for (String dl : dataLines) {
            String[] parts = dl.split(",");
            itemnos.add(Integer.parseInt(parts[0]));
            supplierwhse.add(Integer.parseInt(parts[1]));
            qty.add(Integer.parseInt(parts[2]));
        }

        /*
        Given
         */
        CreateNewOrder(itemnos, supplierwhse, qty);

    }
        private void CreateNewOrder (List < Integer > itemnos, List < Integer > supplierwhse, List < Integer > qty)
        {
            ArrayList<Double> adjusted_qty = new ArrayList<>();
            ArrayList<Double> item_amt = new ArrayList<>();
            ArrayList<String> item_name = new ArrayList<>();

            String get_next_order_number = String.format("SELECT D_NEXT_O_ID FROM wholesale_dev_a.district WHERE D_W_ID = %d AND D_ID = %d", warehouseId, districtId);
            // List<Row> district = session.execute(get_next_order_number).all();
            Row res = session.execute(get_next_order_number).all().get(0);
            long next_order_num = res.getLong("D_NEXT_O_ID");

            String get_district_tax_query = String.format("SELECT D_TAX from wholesale_dev_a.district WHERE D_W_ID = %d AND D_ID = %d", warehouseId, districtId);
            res = session.execute(get_district_tax_query).all().get(0);
            double district_tax = res.getBigDecimal("D_TAX").doubleValue();
            String increase_order_num_query = String.format("UPDATE district SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = %d AND D_ID = %d", warehouseId, districtId);
            session.execute(increase_order_num_query);

            int all_local = 1;
            for(Integer integer : supplierwhse)
            {
                if (integer != warehouseId)
                {
                    all_local = 0;
                    break;
                }
            }

            Date current = new Date();
            String ordertime = formatter.format(current);

            double totalamt = 0;
            for (int i=0; i <noOfItems; i++)
            {
                String check_stock = String.format("SELECT S_QUANTITY from stock WHERE S_W_ID = %d AND S_I_ID = %d", supplierwhse.get(i),itemnos.get(i));
                Row stock_info = session.execute(check_stock).all().get(0);


            }
        }

       // System.out.println("Running New Order Transaction with C_ID= %d, W_ID=%d, D_ID=%d, N=%d \n", customerId, warehouseId, districtId, noOfItems);
       // for (String line : dataLines) {
          //  System.out.println(line);
        }




