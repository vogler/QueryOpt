package tinydb;

/** A table attribute */
public class Attribute {
   /** An integer */
   public static final int TYPE_INT = 0;
   /** A double */
   public static final int TYPE_DOUBLE = 1;
   /** A boolean */
   public static final int TYPE_BOOL = 2;
   /** A string */
   public static final int TYPE_STRING = 3;

   /** Name of the attribute */
   String name;
   /** Type of the attribute */
   int type;
   /** Average size */
   double size = 4.0; // Guess
   /** Number of unique values */
   int uniqueValues = 100; // Guess
   /** Minimum value */
   Object minValue;
   /** Maximum value */
   Object maxValue;
   /** Key attribute? */
   boolean key;
   /** Index available? */
   boolean index;

   /** The name */
   public String getName() { return name; }
   /** The type */
   public int getType() { return type; }
   /** The size */
   public double getSize() { return size; }
   /** The number of unique values */
   public int getUniqueValues() { return uniqueValues; }
   /** The minimum value */
   public Object getMinValue() { return minValue; }
   /** The maximum value */
   public Object getMaxValue() { return maxValue; }
   /** Key attribute */
   public boolean getKey() { return key; }
   /** Index available? */
   public boolean getIndex() { return index; }

   /** Escape */
   private static String escape(String str) {
      if (str==null) return str;
      int len=str.length();
      boolean escape=false;
      for (int index=0;index<len;index++) {
	 char c=str.charAt(index);
	 if ((c==' ')||(c=='\\'))
	    { escape=true; break; }
      }
      if (!escape) return str;
      StringBuffer result=new StringBuffer(len);
      for (int index=0;index<len;index++) {
	 char c=str.charAt(index);
	 if (c==' ') result.append("\\s"); else
	 if (c=='\\') result.append("\\\\"); else
	    result.append(c);
      }
      return result.toString();
   }
   /** Unescape */
   private static String unescape(String str) {
      if (str==null) return str;
      if (str.indexOf('\\')<0) return str;
      int len=str.length();
      StringBuffer result=new StringBuffer(len);
      for (int index=0;index<len;index++) {
	 char c=str.charAt(index);
	 if ((c=='\\')&&(index+1<len)) {
	    c=str.charAt(++index);
	    if (c=='s')
	       result.append(' '); else
	       result.append(c);
	 } else result.append(c);
      }
      return result.toString();
   }


   /** Read */
   void read(String line) throws java.io.IOException
   {
      int split=line.indexOf(' ');
      if (split<0) throw new java.io.IOException("invalid attribute format");
      name=line.substring(0,split); line=line.substring(split+1);

      split=line.indexOf(' ');
      if (split<0) throw new java.io.IOException("invalid attribute format");
      type=Integer.parseInt(line.substring(0,split)); line=line.substring(split+1);

      split=line.indexOf(' ');
      if (split<0) throw new java.io.IOException("invalid attribute format");
      size=Double.parseDouble(line.substring(0,split)); line=line.substring(split+1);

      split=line.indexOf(' ');
      if (split<0) throw new java.io.IOException("invalid attribute format");
      uniqueValues=Integer.parseInt(line.substring(0,split)); line=line.substring(split+1);

      split=line.indexOf(' ');
      if (split<0) throw new java.io.IOException("invalid attribute format");
      minValue=unescape(line.substring(0,split)); line=line.substring(split+1);
      if (type==TYPE_INT) minValue=Integer.valueOf((String)minValue); else
      if (type==TYPE_DOUBLE) minValue=Double.valueOf((String)minValue); else
      if (type==TYPE_BOOL) minValue=Boolean.valueOf((String)minValue);

      split=line.indexOf(' ');
      if (split<0) {
	 maxValue=unescape(line); line="";
      } else {
         maxValue=unescape(line.substring(0,split)); line=line.substring(split+1);
      }
      if (type==TYPE_INT) maxValue=Integer.valueOf((String)maxValue); else
      if (type==TYPE_DOUBLE) maxValue=Double.valueOf((String)maxValue); else
      if (type==TYPE_BOOL) maxValue=Boolean.valueOf((String)maxValue);

      key=false;
      if (line.startsWith("key ")) { key=true; line=line.substring(4); } else
      if (line.equals("key")) { key=true; line=""; }

      index=false;
      if (line.startsWith("index ")) { index=true; line=line.substring(4); } else
      if (line.equals("index")) { index=true; line=""; }
   }

   /** Write */
   void write(java.io.PrintWriter out) throws java.io.IOException
   {
      out.print(name); out.print(" ");
      out.print(type); out.print(" ");

      if ((minValue==null)&&(maxValue==null)) {
	 if (type==TYPE_INT) minValue=maxValue=new Integer(0); else
	 if (type==TYPE_DOUBLE) minValue=maxValue=new Double(0); else
	 if (type==TYPE_BOOL) minValue=maxValue=Boolean.FALSE; else
	    minValue=maxValue="";
      } else if (minValue==null) {
	 minValue=maxValue;
      } else if (maxValue==null) {
	 maxValue=minValue;
      }

      out.print(size); out.print(' ');
      out.print(uniqueValues); out.print(' ');
      out.print(escape(minValue.toString())); out.print(' ');
      out.print(escape(maxValue.toString()));

      if (key) out.print(" key");
      if (index) out.print(" index");
      out.println();
   }
}
