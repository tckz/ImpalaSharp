/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Thrift;
using Thrift.Collections;
using System.Runtime.Serialization;
using Thrift.Protocol;
using Thrift.Transport;

namespace ImpalaSharp.Thrift.Beeswax
{

  /// <summary>
  /// Metadata information about the results.
  /// Applicable only for SELECT.
  /// </summary>
  #if !SILVERLIGHT
  [Serializable]
  #endif
  public partial class ResultsMetadata : TBase
  {
    private ImpalaSharp.Thrift.Hive.Schema _schema;
    private string _table_dir;
    private string _in_tablename;
    private string _delim;

    /// <summary>
    /// The schema of the results
    /// </summary>
    public ImpalaSharp.Thrift.Hive.Schema Schema
    {
      get
      {
        return _schema;
      }
      set
      {
        __isset.schema = true;
        this._schema = value;
      }
    }

    /// <summary>
    /// The directory containing the results. Not applicable for partition table.
    /// </summary>
    public string Table_dir
    {
      get
      {
        return _table_dir;
      }
      set
      {
        __isset.table_dir = true;
        this._table_dir = value;
      }
    }

    /// <summary>
    /// If the results are straight from an existing table, the table name.
    /// </summary>
    public string In_tablename
    {
      get
      {
        return _in_tablename;
      }
      set
      {
        __isset.in_tablename = true;
        this._in_tablename = value;
      }
    }

    /// <summary>
    /// Field delimiter
    /// </summary>
    public string Delim
    {
      get
      {
        return _delim;
      }
      set
      {
        __isset.delim = true;
        this._delim = value;
      }
    }


    public Isset __isset;
    #if !SILVERLIGHT
    [Serializable]
    #endif
    public struct Isset {
      public bool schema;
      public bool table_dir;
      public bool in_tablename;
      public bool delim;
    }

    public ResultsMetadata() {
    }

    public void Read (TProtocol iprot)
    {
      TField field;
      iprot.ReadStructBegin();
      while (true)
      {
        field = iprot.ReadFieldBegin();
        if (field.Type == TType.Stop) { 
          break;
        }
        switch (field.ID)
        {
          case 1:
            if (field.Type == TType.Struct) {
              Schema = new ImpalaSharp.Thrift.Hive.Schema();
              Schema.Read(iprot);
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 2:
            if (field.Type == TType.String) {
              Table_dir = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 3:
            if (field.Type == TType.String) {
              In_tablename = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          case 4:
            if (field.Type == TType.String) {
              Delim = iprot.ReadString();
            } else { 
              TProtocolUtil.Skip(iprot, field.Type);
            }
            break;
          default: 
            TProtocolUtil.Skip(iprot, field.Type);
            break;
        }
        iprot.ReadFieldEnd();
      }
      iprot.ReadStructEnd();
    }

    public void Write(TProtocol oprot) {
      TStruct struc = new TStruct("ResultsMetadata");
      oprot.WriteStructBegin(struc);
      TField field = new TField();
      if (Schema != null && __isset.schema) {
        field.Name = "schema";
        field.Type = TType.Struct;
        field.ID = 1;
        oprot.WriteFieldBegin(field);
        Schema.Write(oprot);
        oprot.WriteFieldEnd();
      }
      if (Table_dir != null && __isset.table_dir) {
        field.Name = "table_dir";
        field.Type = TType.String;
        field.ID = 2;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(Table_dir);
        oprot.WriteFieldEnd();
      }
      if (In_tablename != null && __isset.in_tablename) {
        field.Name = "in_tablename";
        field.Type = TType.String;
        field.ID = 3;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(In_tablename);
        oprot.WriteFieldEnd();
      }
      if (Delim != null && __isset.delim) {
        field.Name = "delim";
        field.Type = TType.String;
        field.ID = 4;
        oprot.WriteFieldBegin(field);
        oprot.WriteString(Delim);
        oprot.WriteFieldEnd();
      }
      oprot.WriteFieldStop();
      oprot.WriteStructEnd();
    }

    public override string ToString() {
      StringBuilder sb = new StringBuilder("ResultsMetadata(");
      sb.Append("Schema: ");
      sb.Append(Schema== null ? "<null>" : Schema.ToString());
      sb.Append(",Table_dir: ");
      sb.Append(Table_dir);
      sb.Append(",In_tablename: ");
      sb.Append(In_tablename);
      sb.Append(",Delim: ");
      sb.Append(Delim);
      sb.Append(")");
      return sb.ToString();
    }

  }

}