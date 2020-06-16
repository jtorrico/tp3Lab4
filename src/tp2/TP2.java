/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tp2;

import com.mongodb.*;
import com.mongodb.client.model.Indexes;
import com.mongodb.operation.OrderBy;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author Jonathan Torrico
 */
public class TP2 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        //Creacion de Cliente Mongo
        MongoClient mongo = new MongoClient("localhost", 27017);
        
        //Creacion de Base de Datos
        DB database = mongo.getDB("paises_db");
        
        //Creacion de la Coleccion dentro de la BD
        database.createCollection("paises", null);
        
        //Creacion de Tabla y Documento para agregar registros a la Coleccion
        DBCollection table = database.getCollection("paises");
        
        //Lista de Metodos de los ejercicios correspondientes.
        //migrate(table);
        //populationFinder(table);
        //notAfricaFinder(table);
        //updateEgypt(table);
        //deleteMozambique(table);
        //regionFinder(table);
        //poblationSel(table);
        //ascSort(table);
        //skip(table);
        //likePattern(table);
        //changeIndex(table);
    }
    
    //Migracion de cada pais a Mongo.
    public static DBCollection migrate (DBCollection table) {
        
        
        for(int codigo = 1; codigo <= 300; codigo++){
            
            try {
                
                String url = "https://restcountries.eu/rest/v2/callingcode/" + codigo;
                URL obj = new URL (url);
                HttpURLConnection con = (HttpURLConnection) obj.openConnection();
                        
                int responseCode = con.getResponseCode();
                
                if (responseCode == 200) {
                    
                    System.out.println("Guardando Pais Numero: " + codigo);
                    BasicDBObject document = new BasicDBObject();
                    BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                    String inputLine;
                    StringBuffer response = new StringBuffer();
			
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
            
                    in.close();
            
                    JSONArray myResponse = new JSONArray(response.toString());
                        
                    JSONObject pais = new JSONObject(myResponse.getJSONObject(0).toString());
                    JSONArray llamada = new JSONArray (pais.getJSONArray("callingCodes").toString());
                    JSONArray latLng = new JSONArray (pais.getJSONArray("latlng").toString());
                       
                    document.put("codigoPais", llamada.getInt(0));
                    document.put("nombrePais", pais.get("name"));
                    document.put("capitalPais", pais.get("capital"));
                    document.put("region", pais.get("region"));
                    document.put("poblacion", pais.get("population"));
                    document.put("latitud", latLng.get(0));
                    document.put("longitud", latLng.get(1));
                    document.put("superficie", pais.get("area"));
                    table.insert(document);
                    
                } else {
                    continue;
                }    
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        
        return table;
    }
    
    
    //Query para buscar los paises con region en Africa
    public static void regionFinder (DBCollection table){
        
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("region", "Africa");
        DBCursor cursor = table.find(searchQuery);
        
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }
    
    //Query para buscar los paises con poblacion mayor de 100000000.
    public static void populationFinder (DBCollection table){
        
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("poblacion", new BasicDBObject("$gt", 100000000));
        DBCursor cursor = table.find(searchQuery);
        System.out.println("Lista Paises Con Poblacion 100000000");
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }
    
    //Query para buscar los paises que no son de Africa.
    public static void notAfricaFinder (DBCollection table) {
        
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("region", new BasicDBObject("$ne", "Africa"));
        DBCursor cursor = table.find(searchQuery);
        System.out.println("Lista Paises que no son de Africa");
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }
    
    //Metodo update para Egypt y su poblacion.
    public static DBCollection updateEgypt (DBCollection table) {
        
        BasicDBObject nuevoDoc = new BasicDBObject();
        nuevoDoc.put("$set", new BasicDBObject("nombrePais", "Egipto").append("poblacion", 95000000));
        BasicDBObject query = new BasicDBObject().append("codigoPais", 20);
        table.update(query, nuevoDoc);
        
        return table;
    }
    
    //Metodo para eliminar Mozambique de la BD.
    public static DBCollection deleteMozambique (DBCollection table) {
        
        BasicDBObject docDel = new BasicDBObject();
        docDel.put("codigoPais", 258);
        table.remove(docDel);
        
        return table;
    }
    
    //Query para buscar los paises con poblacion entre 50000000 y 150000000.
    public static DBCollection poblationSel (DBCollection table) {
     
        BasicDBObject query = new BasicDBObject();
        query.put("poblacion", new BasicDBObject("$gt", 50000000).append("$lt", 150000000));
        DBCursor cursor = table.find(query);
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        
        return table;
    }
    
    //Query para mostrar los registros en forma ascendente.
    public static DBCollection ascSort (DBCollection table) {
        
        DBCursor cursor = table.find().sort(new BasicDBObject("codigoPais", OrderBy.ASC.getIntRepresentation()));
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        
        return table;
    }
    
    //Query para mostrar los registros, saltando los primeros 5.
    public static DBCollection skip (DBCollection table){
        
        DBCursor cursor = table.find().sort(new BasicDBObject("nombrePais", OrderBy.ASC.getIntRepresentation())).skip(5);
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        
        return table;
    }
    
    //Query para mostrar los paises que en su nombre contengan Ar, usando regex.
    public static DBCollection likePattern (DBCollection table) {
        
        BasicDBObject query = new BasicDBObject();
        Pattern p = Pattern.compile("Ar");
        query.put("nombrePais", p);
        
        DBCursor cursor = table.find(query);
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        
        return table;
    }
    
    //Metodo para agregar un Index con el codigoPais.
    public static DBCollection changeIndex(DBCollection table){
        
        table.createIndex(new BasicDBObject ("codigoPais", 1));
        
        return table;
    }
    
}
