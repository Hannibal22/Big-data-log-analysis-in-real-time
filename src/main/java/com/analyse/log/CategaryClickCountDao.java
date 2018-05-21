package com.analyse.log;

import com.analyse.site.HbaseUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CategaryClickCountDao {


    public List<CategaryCount> query(String day) throws Exception {
        List<CategaryCount> list = new ArrayList<CategaryCount>();

       Map<String,Long> map =  HbaseUtils.getInstance().query("search_log_count",day);
        for (Map.Entry<String,Long> entry: map.entrySet()) {
            CategaryCount categaryClickCount = new CategaryCount();
            categaryClickCount.setName(entry.getKey());
            categaryClickCount.setValue(entry.getValue());
            list.add(categaryClickCount);
        }


        return list;
    }

    /**
     *
     */
    public static void main(String args[]) throws Exception
    {
        CategaryClickCountDao categaryClickCountDao = new CategaryClickCountDao();
        List<CategaryCount> list = categaryClickCountDao.query("2018");
        for (CategaryCount c: list) {
            System.out.println(c.getValue());
        }
    }
}
