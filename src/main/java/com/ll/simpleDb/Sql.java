package com.ll.simpleDb;

import com.ll.Article;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;

public class Sql {
    private StringBuilder sb = new StringBuilder();
    private SimpleDb simpleDb;
    private List<Object> paramList = new ArrayList<>();

    public Sql(SimpleDb simpleDb) {
        this.simpleDb = simpleDb;
    }


    public Sql append(String sqlPart,Object... param) {
        sb.append(sqlPart);
        sb.append(" ");

        for(Object p : param) {
            paramList.add(p);
        }
        return this;
    }

    public long insert() {
        long id = simpleDb.insert(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return id;
    }

    public int update() {
        int RowsCount = simpleDb.update(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return RowsCount;
    }

    public int delete() {
        int RowsCount = simpleDb.update(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return RowsCount;
    }

    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> articleRow = simpleDb.selectRows(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return articleRow;
    }

    public Map<String, Object> selectRow() {
        Map<String, Object> articleRow = simpleDb.selectRow(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return articleRow;
    }

    public LocalDateTime selectDatetime() {
        LocalDateTime datetime = simpleDb.selectDatetime(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return datetime;
    }

    public Long selectLong() {
        Long longid = simpleDb.selectLong(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return longid;
    }

    public String selectString() {
        String title = simpleDb.selectString(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return title;
    }

    public Boolean selectBoolean() {
        Boolean isBlind = simpleDb.selectBoolean(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return isBlind;
    }

    public Sql appendIn(String sqlPart,Object... param) {
        StringBuilder placeholder = new StringBuilder();

        for(int i = 0; i < param.length; i++) {
            if(i > 0) {
                placeholder.append(", ");
            }
            placeholder.append("?");
        }

        String newsqlPart = sqlPart.replace("?", placeholder);

        sb.append(newsqlPart);
        sb.append(" ");

        for(Object p : param) {
            paramList.add(p);
        }
        return this;
    }

    public List<Long> selectLongs() {
        List<Long> foundIds = simpleDb.selectLongs(sb.toString(), paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return foundIds;
    }

    public <T> List<T> selectRows(Class<T> cls){
        List<T> rows = simpleDb.selectRows(sb.toString(), cls, paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return rows;
    }

    public <T> T selectRow(Class<T> cls){
        T row = simpleDb.selectRow(sb.toString(), cls, paramList.toArray());

        sb.setLength(0);
        paramList.clear();

        return row;
    }
}
