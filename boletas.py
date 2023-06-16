# -*- coding: utf-8 -*-

class boletas:
        def validarRut(self, rut):
                rut = rut.upper();
                rut = rut.replace("-","")
                rut = rut.replace(".","")
                aux = rut[:-1]
                dv = rut[-1:]

                revertido = map(int, reversed(str(aux)))
                factors = cycle(range(2,8))
                s = sum(d * f for d, f in zip(revertido,factors))
                res = (-s)%11

                if str(res) == dv:
                        return True
                elif dv=="K" and res==10:
                        return True
                else:
                        return False

        def __decode_string(self, s, input_string, encoding = 'utf-8'):
                if 1== 1: #try:
                        LOG2(s, 'Input String: %s' % input_string)
                        return unicode(base64.b64decode(input_string), encoding)
                #except Exception as e:
                #       return ""


        def __encode_string(self, s, input_string, encoding = 'utf-8'):
                #return input_string.encode('base64')
                return base64.b64encode(input_string)


        def __convert_dict_base64_to_utf(self, s, fields, dict_target):
                for field in fields:
                        LOG2(s, 'Campo: %s' % field)
                        if field in dict_target:
                                dict_target[field] = self.__decode_string(s, dict_target[field])
                return dict_target


        def __valida_numerico(self, dict_payload, fields):
                temp = []
                for field in fields:
                        if field in dict_payload:
                                if not (type(dict_payload[field]) in (type(1), type(1.0)) or dict_payload[field].isdigit()) :
                                        temp.append(field)
                                elif float(dict_payload[field]) == 0.:
                                        temp.append(field)
                return temp


        def __valida_alfanumerico(self, dict_payload, fields):
                temp = []
                for field in fields:
                        if field in dict_payload:
                                if len(dict_payload[field] or '') == 0:
                                        temp.append(field)
                return temp


        def __valida_fecha(self, dict_payload, fields):
                temp = []
                for field in fields:
                        if field in dict_payload:
                                try:
                                        datetime.datetime.strptime(dict_payload[field], '%d/%m/%Y')
                                except Exception as e:
                                        temp.append(field)
                return temp


        def __getCampo(self, payload, field):
                if field in payload:
                        return payload[field]
                else:
                        return ''


        def __execute(self,s, query_string, par_bd1):
                ip_bd1=par_bd1.split(' -h ')[1].split(' ')[0]
                port_bd1=int(par_bd1.split(' -p ')[1].split(' ')[0])
                result = query_db_json(ip_bd1, port_bd1, query_string)
                return result

        def __dato_adicional(self, xml_inicial, campo, valor):
                return '%s<DatosAdjuntos><NombreDA>%s</NombreDA><ValorDA>%s</ValorDA></DatosAdjuntos>' % (xml_inicial, campo, valor)


        def __xml_adicionales(self, payload):
                campos = ['GE_Monto_Neto', 'GE_Monto_IVA', 'GE_MEDIO_PAGO', 'GE_NOM_FANTASIA', 'GE_DIR_COM', 'GE_CIUDAD_COM', 'GE_COD_COMERCIO', 'GE_
VER_POS', 'GE_FECHA_OPERACION', 'GE_HORA_OPERACION', 'GE_TERMINAL', 'GE_NUM_TARJETA', 'GE_FORMA_OPERACION', 'GE_PROPINA', 'GE_VUELTO', 'GE_TOTAL', 'G
E_EMPLEADO', 'GE_NUM_OPERACION', 'GE_COD_AUTORIZACION', 'GE_TASA_INTERES', 'GE_GLOSA_CUOTA', 'GE_GLOSA_CUOTA_2', 'GE_GLOSA_PROMOCION', 'GE_TIPO_MONED
A', 'GE_SALDO_PREPAGO', 'GE_GLOSA_PREPAGO', 'GE_FECHA_CONTABLE', 'GE_NUM_CUENTA', 'GE_TIPO_CUOTA', 'GE_VAL_CUOTA_1', 'GE_VAL_CUOTA_2', 'GE_VAL_CUOTA_
3', 'GE_VAL_CUOTA_DIF_1', 'GE_TASA_DIF_2', 'GE_VAL_CUOTA_DIF_2', 'GE_TASA_DIF_3', 'GE_VAL_CUOTA_DIF_3', 'GE_NUM_CUOTAS', 'GE_CANAL', 'GE_GLOSA_SURCHA
RGE', 'GE_LABEL_ID', 'GE_APLICATION_ID', 'GE_RUT_RECEPTOR', 'GE_NOMBRE_RECEPTOR', 'GE_DIRECCION_RECEPTOR', 'GE_COMUNA_RECEPTOR', 'GE_CIUDAD_RECEPTOR'
, 'GE_EMAIL_RECEPTOR']
                xml = ''
                for campo in campos:
                        if campo in payload:
                                if payload[campo] <> '' and payload[campo] != None:
                                        xml = self.__dato_adicional(xml, campo, payload[campo])
                return xml


        def __inserta_en_cola(self,s, canal, folio, rut_emisor, rut_receptor, tipo_dte, json_emitir, session, id_gestor_folio, idtx):
                par_bd1=var_global["CONEXION_API_BASE_COLAS"]
                json_servicio = {}
                json_servicio['CANAL'] = canal
                json_servicio['RUT_EMISOR'] = rut_emisor
                json_servicio['RUT_RECEPTOR'] = rut_receptor
                json_servicio['TIPO_DTE'] = tipo_dte
                json_servicio['JSON_EMITIR'] = json_emitir
                json_servicio['SESSION'] = session
                json_servicio['FOLIO'] = folio
                json_servicio['ID_GESTOR_FOLIOS'] = id_gestor_folio
                json_servicio['__ID_TX_TBK__']=idtx

                LOG2(s, 'Antes de exe inserta_colas_tbk' + json.dumps(json_servicio))

                db_out = self.__execute(s,"select inserta_colas_tbk('%s')" % json.dumps(json_servicio),par_bd1)
                LOG2(s, 'despues del exe inserta' +  json.dumps(db_out))
                LOGDEBUG(s, 'despues del exe inserta' +  json.dumps(db_out))
                if 'STATUS' not in db_out or db_out['STATUS']!="OK":
                        LOG2(s,'Falla Insertar en las colas, probamos otro site')
                        par_bd1=var_global["CONEXION_API_BASE_COLAS_OTROSITE"]
                        db_out = self.__execute(s,"select inserta_colas_tbk('%s')" % json.dumps(json_servicio),par_bd1)
                        if 'STATUS' not in db_out or db_out['STATUS']!="OK":
                                LOG2(s,'Error execute ' + str(db_out))
                                raise Exception('Error al insertar en BD')

        def __creaJsonBoleta(self, fromTBK):
                temp = {}
                temp['dispositivo'] = 'computador',
                temp['tipoDTE'] = str(fromTBK['GE_TIPO_DTE'])
                temp['TipoDTE'] = str(fromTBK['GE_TIPO_DTE'])
                temp['CdgSIISucur'] = '080374629'
                temp['FchEmis'] = '%s-%s-%s' % (fromTBK['GE_FECHA_EMI'][6:10], fromTBK['GE_FECHA_EMI'][3:5], fromTBK['GE_FECHA_EMI'][0:2])
                temp['FchVenc'] = ''
                temp['FmaPago'] = ''
                temp['Acteco'] = ''
                temp['Actecos'] = ''
                temp['TpoTranCompra'] = ''
                temp['TpoTranVenta'] = ''
                temp['TipoFactEsp'] = ''
                temp['IndMntNeto'] = ''
                temp['IndServicio'] = '3'
                temp['IndTraslado'] = ''
                temp['CdgTraslado'] = ''
                temp['RUTRecep'] =  fromTBK['GE_RUT_RECEPTOR']
                temp['RznSocRecep'] = self.__getCampo(fromTBK, 'GE_NOMBRE_RECEPTOR')
                temp['NumId'] = ''
                temp['TipoDocID'] = ''
                temp['GiroRecep'] = ''
                temp['CodIntClie'] = ''
                temp['DirRecep'] = self.__getCampo(fromTBK, 'GE_DIRECCION_RECEPTOR')
                temp['region'] = 'RM'
                temp['CmnaRecep'] = self.__getCampo(fromTBK, 'GE_COMUNA_RECEPTOR')
                temp['CiudadRecep'] = self.__getCampo(fromTBK, 'GE_CIUDAD_RECEPTOR')
                temp['CorreoRecep'] = self.__getCampo(fromTBK, 'GE_EMAIL_RECEPTOR')
                temp['Contacto'] = ''
                temp['boletaTiposerv'] = '3',
                temp['jsonDetalleLiqFactura'] = '[]'
                temp['jsonDetalleDocsExportacion'] = '[]'
                temp['ImptoReten'] = []
                temp['jsonDetalle'] = [
                        {
                                "PrcConsFinal": "",
                                "UnmdItem": "Medida",
                                "VlrCodigo": "Codigo de Producto",
                                "MontoImp": "DescripciÃ³n",
                                "NroLinDet": "#",
                                "PrcItem": "Precio",
                                "CodImpAdic": "Impuesto RetenciÃ³n",
                                "ivaProducto": "IVA",
                                "MntBaseFaena": "CÃ³digo Item",
                                "NmbItem": "Producto",
                                "TpoCodigo": "CÃ³digo Auxiliar",
                                "MontoItem": "Monto Item",
                                "DescuentoMonto": "Descuento",
                                "QtyItem": "Cantidad",
                                "DescuentoPct": "Tipo Descuento"
                        },
                        {
                                'NroLinDet': '1',
                                'IndExe': '1' if str(fromTBK['GE_TIPO_DTE']) == '41' else '',
                                'VlrCodigo': 'Venta al Detalle Exento' if str(fromTBK['GE_TIPO_DTE'])=='41' else 'Venta al Detalle Afecto',
                                'TpoCodigo': '',
                                'NmbItem': 'Venta al Detalle Exento' if str(fromTBK['GE_TIPO_DTE'])=='41' else 'Venta al Detalle Afecto',
                                'QtyItem': '1',
                                'UnmdItem': '',
                                'PrcItem': fromTBK['GE_MONTO'],
                                'DescuentoPct': "0",
                                'DescuentoMonto': "",
                                'MontoItem': fromTBK['GE_MONTO'],
                                'ivaProducto': "IVA",
                                'DscItem': "Descripción",
                                'CodItem': "Código Item",
                                'RUTMandante': ""
                        }]
                temp['jsonReferencias'] = []
                temp['Patente'] = ''
                temp['RUTTrans'] = ''
                temp['RUTChofer'] = ''
                temp['NombreChofer'] = ''
                temp['DirDest'] = ''
                temp['CmnaDest'] = ''
                temp['CiudadDest'] = ''
                temp['RegionDest'] = ''
                temp['TipoDespacho'] = ''
                temp['CodModVenta'] = ''
                temp['CodClauVenta'] = ''
                temp['TotClauVenta'] = ''
                temp['CodPaisDestin'] = ''
                temp['CodPaisRecep'] = ''
                temp['RUTCiaTransp'] = ''
                temp['NombreTransp'] = ''
                temp['CodViaTransp'] = ''
                temp['PesoBruto'] = ''
                temp['CodUnidPesoBruto'] = ''
                temp['CodPtoEmbarque'] = ''
                temp['IdAdicPtoEmb'] = ''
                temp['CodPtoDesemb'] = ''
                temp['jsonBultos'] = []
                temp['TotBultos'] = ''
                temp['eti1'] = 'Origen-TBK'
                temp['eti2'] = ''
                temp['eti3'] = ''
                temp['observaciones'] = ''
                temp['TpoMoneda'] = ''
                temp['DscRcgGlobal'] = ''
                temp['subTotal'] = fromTBK['GE_TOTAL']
                temp['tipoDesc'] = "Porcentaje"
                #temp['MntExe'] = ''
                temp['MntExe'] = fromTBK['GE_MONTO'] if fromTBK['GE_TIPO_DTE'] == '41' else ''
                if 'GE_Monto_Neto' in fromTBK:
                        temp['MntNeto'] = self.__getCampo(fromTBK, 'GE_Monto_Neto')
                if 'GE_Monto_IVA' in fromTBK:
                        temp['IVA'] = self.__getCampo(fromTBK, 'GE_Monto_IVA')
                temp['MntTotal'] = fromTBK['GE_MONTO']
                temp['TpoMoneda2'] = ''
                temp['TpoCambio'] = ''
                temp['Glosa'] = ''
                temp['TipoMovim'] = ''
                temp['MntExeOtrMnda'] = ''
                temp['MntTotOtrMnda'] = ''
                temp['XML_Adicionales'] = self.__xml_adicionales(fromTBK)
                temp['parametro5'] = fromTBK['GE_ID_UNICO']
                return temp


        def creaBoleta(self, data):
                par_bd1=var_global["CONEXION_API_BASE_COLAS"]
                s = data['self']
                rut_no_ceros=data["data"]['GE_RUT_EMI'].lstrip('0')
                data["data"]['GE_RUT_EMI']=rut_no_ceros


                tabla_trx = 'transacciones_tbk'
                LOG2(s,'Origen ='+str(data['server'][0]))
                if str(data['server'][0])=='127.0.0.1':
                        tabla_trx = 'transacciones_tbk_cert'
                if str(data['server'][0])=='172.16.14.116':
                        tabla_trx = 'transacciones_tbk_qa'

                #MVG 40247470
                idtx = None
                if 'flag_reintento' in data['data']:
                        idtx = data['data']['idtx']
                        if 'tabla_trx' in data['data']:
                                tabla_trx = data['data']['tabla_trx']
                                LOG2(s,'Es un reintento en la tabla tabla_trx='+str(tabla_trx))
                        LOG2(s,'Es un reintento idxt='+str(idtx))
                        if idtx.isdigit() is False:
                                return self.__responde(s, 'Reintente por Favor', {'GS_CODIGO_RESPUESTA': '99', 'GS_MENSAJE_RESPUESTA': 'Falla reinten
to'}, 200)

                jout = {}
                #Grabamos la transaccion
                if idtx is None:
                        LOG2(s, "insert into "+tabla_trx+"(id,fecha_ingreso,estado,rut_emisor,id_tbk,input) values(default,now(),'EN_PROCESO','"+str(
data["data"].get('GE_RUT_EMI'))+"','"+str(data["data"].get('GE_ID_UNICO'))+"','"+json.dumps(data["data"])+"') returning id")
                        jtx1 = self.__execute(s,"insert into "+tabla_trx+"(id,fecha_ingreso,estado,rut_emisor,id_tbk,input) values(default,now(),'EN_
PROCESO','"+str(data["data"].get('GE_RUT_EMI'))+"','"+str(data["data"].get('GE_ID_UNICO'))+"','"+json.dumps(data["data"])+"') returning id",par_bd1)
                        if "STATUS" not in jtx1 or jtx1["STATUS"]!="OK":
                                LOG2(s,'Falla insertar transaccion probamos otro site')
                                par_bd1=var_global["CONEXION_API_BASE_COLAS_OTROSITE"]
                                jtx1 = self.__execute(s,"insert into "+tabla_trx+"(id,fecha_ingreso,estado,rut_emisor,id_tbk,input) values(default,no
w(),'EN_PROCESO','"+str(data["data"].get('GE_RUT_EMI'))+"','"+str(data["data"].get('GE_ID_UNICO'))+"','"+json.dumps(data["data"])+"') returning id",p
ar_bd1)
                                if "STATUS" not in jtx1 or jtx1["STATUS"]!="OK":
                                        return self.__responde(s, 'Reintente por Favor', {'GS_CODIGO_RESPUESTA': '99', 'GS_MENSAJE_RESPUESTA': self._
_encode_string(s, 'Falla Insertar Transaccion'), 'GS_NUMERO_FOLIO': '', 'GS_ID_UNICO_TBK': str(data["data"].get('GE_ID_UNICO')), 'GS_TED': ''}, 200)
                                idtx=jtx1["id"]
                        else:
                                idtx=jtx1["id"]

                id_folio = ''
                id_unico = ''
                ted = ''
                error_codigo = '00'
                error_mensaje = ''
                error_mensaje_interno = ''
                cliActTbk = None
                campos_obligatorios = ['GE_TIPO_OPERACION', 'GE_TIPO_DTE', 'GE_FECHA_EMI', 'GE_RUT_EMI', 'GE_RAZON_SOCIAL', 'GE_NOM_ITEM', 'GE_MONTO'
, 'GE_ID_UNICO', 'GE_MEDIO_PAGO', 'GE_NOM_FANTASIA', 'GE_DIR_COM', 'GE_CIUDAD_COM', 'GE_COD_COMERCIO', 'GE_VER_POS', 'GE_FECHA_OPERACION', 'GE_HORA_O
PERACION', 'GE_TERMINAL', 'GE_TOTAL', 'GE_NUM_OPERACION', 'GE_TIPO_MONEDA', 'GE_CANAL'] #, 'GE_LABEL_ID'] # , 'GE_RUT_RECEPTOR'] #, 'GE_NOMBRE_RECEPT
OR', 'GE_DIRECCION_RECEPTOR', 'GE_COMUNA_RECEPTOR', 'GE_CIUDAD_RECEPTOR', 'GE_EMAIL_RECEPTOR']  #, 'GE_Monto_Neto', 'GE_Monto_IVA']
                campos_base64 = ['GE_RAZON_SOCIAL', 'GE_NOM_ITEM', 'GE_ID_UNICO', 'GE_NOM_FANTASIA', 'GE_DIR_COM', 'GE_CIUDAD_COM', 'GE_TERMINAL', 'G
E_FORMA_OPERACION', 'GE_COD_AUTORIZACION', 'GE_TASA_INTERES', 'GE_GLOSA_CUOTA', 'GE_GLOSA_CUOTA_2', 'GE_GLOSA_PROMOCION', 'GE_TIPO_CUOTA', 'GE_TASA_D
IF_2', 'GE_TASA_DIF_3', 'GE_GLOSA_SURCHARGE', 'GE_LABEL_ID', 'GE_RUT_RECEPTOR', 'GE_NOMBRE_RECEPTOR', 'GE_DIRECCION_RECEPTOR', 'GE_COMUNA_RECEPTOR',
'GE_CIUDAD_RECEPTOR', 'GE_EMAIL_RECEPTOR']
                try:
                        if 'HEADERS' in data:
                                #for _ in data['server']:
                                #       LOG2(s,'server ='+str(_)+' VALUE='+data['server'][_])
                                if 'Authorization' in data['HEADERS']:
                                        if data['HEADERS']['Authorization'] != 'Basic dHJhbnNiYW5rOmFjZXB0YTIwMjA=':
                                                raise Exception('401')
                                else:
                                        raise Exception('401')
                        else:
                                raise Exception('401')

                        payload = data['data']
                        #payload = json.loads(payload)
                        LOG2(s, 'Datos de entrada completos' + json.dumps(payload))

                        # Validacion rut emisor valido
                        error_rut_emi = payload['GE_RUT_EMI']
                        #if self.validarRut(error_rut_emi) == False:
                                #error_mensaje = 'El RUT emisor enviado no es valido'
                                #raise Exception('31')

                        #cliActTbk = self.__execute(s,"select rut_emisor from maestro_clientes where rut_emisor='"+str(data["data"].get('GE_RUT_EMI')
).split('-')[0]+"' AND flag_tbk = 'SI'",par_bd1)
                        #LOG2(s, 'Validando si la empresa TBK esta habilitada' + json.dumps(cliActTbk))
                        #if "STATUS" not in cliActTbk or cliActTbk["STATUS"]!="OK":
                                #error_mensaje = 'El RUT emisor enviado no es cliente Transbank'
                                #raise Exception('32')
                        if 'GE_ID_UNICO' in payload:
                                id_unico = payload['GE_ID_UNICO']
                        # Valido Campos Obligatorios
                        if not all(keys in payload.keys() for keys in campos_obligatorios):
                                # error_mensaje = 'Faltan los siguientes campos: %s' % ', '.join(set(campos_obligatorios) - set(payload.keys()))
                                error_mensaje = 'Omisión de campos en el JSON'
                                LOG2(s, 'Datos que faltan' + ', '.join(set(campos_obligatorios) - set (payload.keys())))
                                raise Exception('11')
                        payload['GE_TIPO_DTE'] = str(payload['GE_TIPO_DTE'])
                        if payload['GE_TIPO_DTE'] == '39':
                                campos_obligatorios.append('GE_Monto_IVA')
                                campos_obligatorios.append('GE_Monto_Neto')
                        elif payload['GE_TIPO_DTE'] == '41':
                                payload.pop('GE_Monto_IVA', None)
                                payload.pop('GE_Monto_Neto', None)
                        # Valido que los campos numericos contienen numeros
                        error_numerico = self.__valida_numerico(payload, ['GE_MONTO', 'GE_Monto_IVA', 'GE_Monto_Neto'])
                        if len(error_numerico) > 0:
                                error_mensaje = 'Los siguientes campos deben ser numericos y valor mayor a 0: %s' % ', '.join(error_numerico)
                                raise Exception('13')
                        # Convertimos campos base64 a utf-8
                        payload = self.__convert_dict_base64_to_utf(s, campos_base64, payload)
                        # Validamos que los campos no esten vacios
                        LOG2(s, 'GE_RAZON_SOCIAL ' + payload['GE_RAZON_SOCIAL'])
                        LOG2(s, 'GE_NOM_ITEM ' + payload['GE_NOM_ITEM'])
                        LOG2(s, 'GE_RUT_EMI ' + payload['GE_RUT_EMI'])
                        LOG2(s, 'GE_ID_UNICO ' + payload['GE_ID_UNICO'])

                        error_alfanumerico = self.__valida_alfanumerico(payload, ['GE_RAZON_SOCIAL', 'GE_NOM_ITEM', 'GE_RUT_EMI', 'GE_ID_UNICO'])
                        if len(error_alfanumerico) > 0:
                                error_mensaje = 'Los siguientes campos estan vacios: %s' % ', '.join(error_alfanumerico)
                                raise Exception('14')
                        # Validamos el formato de las fechas
                        error_fecha = self.__valida_fecha(payload, ['GE_FECHA_EMI', 'GE_FECHA_CONTABLE'])
                        if len(error_fecha) > 0:
                                error_mensaje = 'Los siguientes campos no tienen el formato valido (DD/MM/AAAA): %s' % ', '.join(error_fecha)
                                raise Exception('15')

                        # JSON para obtener el TED
                        session = payload['GE_ID_UNICO']
                        if 'GE_RUT_RECEPTOR' in payload:
                                if payload['GE_RUT_RECEPTOR'] in ['', None]:
                                        payload['GE_RUT_RECEPTOR'] = '66666666-6'
                        else:
                                payload['GE_RUT_RECEPTOR'] = '66666666-6'
                        json_boleta = self.__creaJsonBoleta(payload)
                        data_flujo = {}
                        #data_flujo['tipo_tx'] = 'emitir_documento_firmado'
                        data_flujo['tipo_tx'] = 'get_ted_tbk'
                        data_flujo['formEmitirdocumento'] = json_boleta #es para cuando se hace emitir directo
                        data_flujo['session_id'] = session
                        #Rut se cambia dentro del flujo, por el usuario autorizado para emision
                        data_flujo['rutUsuario'] = '22436914'
                        data_flujo['APP'] = 'emitir_v3'
                        data_flujo['pass'] = '__CLAVE_CENTRALIZADA_ESC__'
                        data_flujo['rutCliente'] = payload['GE_RUT_EMI'].split('-')[0].replace('.','')
                        data_flujo['LLAMA_FLUJO_AL_ENTRAR'] = 'SI'
                        #Dentro del fulo 6001 va directo a la secuencia 57, al flujo 12794
                        data_flujo['SECUENCIA_FLUJO'] = '57'
                        data_flujo['__ID_FORM__'] = '0'
                        data_flujo['__SOLO_TED__'] = 'SI'
                        data_flujo['__TBK__'] = 'SI'

                        LOG2(s, '[crea Boleta] Query TED: http://interno.acepta.com:8080/tbk_interno%s' % json.dumps(data_flujo))
                        #r = requests.post('http://interno.acepta.com:8080/tbk_interno', data = json.dumps(data_flujo))
                        r = requests.post('http://motor-cert.acepta.com/interno', data = json.dumps(data_flujo))
                        #r = curl_python("http://motor-cert.acepta.com/interno",json.dumps(data_flujo))
                        LOG2(s, '[crea Boleta] Respuesta TED: %s' % r)
                        LOG2(s, '[crea Boleta] Respuesta TED: %s %s' % (r.status_code, r.content))
                        #if r['status'] == 200:
                        if r.status_code == 200:
                                #respuesta = json.loads(r['output'])
                                respuesta = r.json()
                                if respuesta['CODIGO_RESPUESTA'] == '1':
                                        ted = respuesta['RESPUESTA']['TED']
                                        #ted = ted[:ted.find('<TmstFirma>')]
                                        #ted = self.__encode_string(s, ted).replace('\n', '')
                                        #raise Exception('Prueba error 99')   ### Para probar error de conexion timeout tbk
                                        id_folio = respuesta['RESPUESTA']['FOLIO']
                                        id_gestor_folio = respuesta['RESPUESTA']['ID_GESTOR_FOLIO']
                                        self.__inserta_en_cola(s,'escritorio.acepta.com', id_folio, payload['GE_RUT_EMI'][:-2], payload['GE_RUT_RECEP
TOR'][:-2], payload['GE_TIPO_DTE'], json.dumps(json_boleta), session, id_gestor_folio, idtx)
                                #DAO 20201026, respondemos por proxy
                                elif respuesta['CODIGO_RESPUESTA'] == '4':
                                        ted = respuesta['RESPUESTA']['TED']
                                        id_folio = respuesta['RESPUESTA']['FOLIO']
                                        id_gestor_folio = respuesta['RESPUESTA']['ID_GESTOR_FOLIO']
                                        error_mensaje_interno = respuesta['MENSAJE_RESPUESTA']

                                        #FGE 20220510, revisamos si se envio a emitir o no
                                        LOG2(s, "select id, estado, fecha_encolamiento from transacciones_tbk where id_tbk = '%s' order by id desc li
mit 1" %self.__encode_string(s, str(data["data"].get('GE_ID_UNICO'))))
                                        jtx1 = self.__execute(s,"select id, estado, fecha_encolamiento from transacciones_tbk where id_tbk = '%s' ord
er by id desc limit 1" %self.__encode_string(s, str(data["data"].get('GE_ID_UNICO'))), par_bd1)
                                        LOG2(s, " Result query transacciones_tbk"+str(jtx1))
                                        if 'STATUS' not in jtx1 or jtx1['STATUS'] != 'OK':
                                                LOG2(s,'Falla busqueda transaccion')
                                                return self.__responde(s, 'Reintente por Favor', {'GS_CODIGO_RESPUESTA': '99', 'GS_MENSAJE_RESPUESTA'
: self.__encode_string(s, 'Falla Reintento Transaccion'), 'GS_NUMERO_FOLIO': '', 'GS_ID_UNICO_TBK': str(data["data"].get('GE_ID_UNICO')), 'GS_TED': '
'}, 200)
                                        #if jtx1['fecha_encolamiento'] == '':
                                        #        id_folio = respuesta['RESPUESTA']['FOLIO']
                                        #        id_gestor_folio = respuesta['RESPUESTA']['ID_GESTOR_FOLIO']
                                        #        self.__inserta_en_cola(s,'escritorio.acepta.com', id_folio, payload['GE_RUT_EMI'][:-2], payload['GE_
RUT_RECEPTOR'][:-2], payload['GE_TIPO_DTE'], json.dumps(json_boleta), session, id_gestor_folio, idtx)

                                elif respuesta['CODIGO_RESPUESTA'] == '2':
                                        raise Exception(respuesta['MENSAJE_RESPUESTA'])
                                elif respuesta['CODIGO_RESPUESTA'] == '3':
                                        documento = get_xml_content(respuesta['RESPUESTA']['URL_DOC'])['content']
                                        id_folio = respuesta['RESPUESTA']['FOLIO']
                                        ted = __encode_string(documento[documento.find('<TED'):documento.find('</TED>') + 6])
                                else:
                                        raise Exception(r.content)
                        else:
                                raise Exception("Falla Obtener TED")
                except Exception as e:
                        error = str(e)
                        LOG2(s,'ERROR COMPLETO: %s ' % error)
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        LOG2(s, 'ERROR: %s %s %s %s' % (error,exc_type, fname, str(exc_tb.tb_lineno)))

                        if error == '401':
                                # resp.status = falcon.HTTP_401
                                error_codigo = '99'
                                error_mensaje = 'Falla autenticación'
                        elif error.startswith('Expecting , delimiter: line'):
                                error_codigo = '10'
                                error_mensaje = 'Problemas en el formato JSON'
                        elif error.startswith('No JSON object could be decoded'):
                                error_codigo = '12'
                                error_mensaje = 'Problemas en el formato JSON'
                        elif error.startswith('Cliente') and error.endswith('maestro_clientes'):
                                error_codigo = '20'
                                error_mensaje = 'Cliente no habilitado para la emisión'
                        elif error.startswith('Cliente') and error.endswith('contribuyentes'):
                                error_codigo = '21'
                                error_mensaje = 'Cliente no está en la lista de contribuyentes'
                        elif error.startswith('CAF: mensaje que detalla el problema.'):
                                error_codigo = '22'
                                error_mensaje = 'Problemas al obtener folio'
                        elif error.startswith('Error en la asignación de folios, Emita Nuevamente'):
                                error_codigo = '23'
                                error_mensaje = 'Problemas al obtener folio'
                        elif error.startswith('Falla Firma CAF, Reintente Por Favor'):
                                error_codigo = '24'
                                error_mensaje = 'Falla al firmar TED del documento'
                        elif error.startswith('Falla Inserción no existe patrón de DTE'):
                                error_codigo = '25'
                                error_mensaje = 'Tipo de DTE no es válido'
                        elif error.startswith('CAF: Sin Folios Disponibles') and error.find('Cliente MiPyme')>0:
                                error_codigo = '29'
                                error_mensaje = 'Problemas al obtener folio. Cliente MiPyme'
                                x = payload["GE_RUT_EMI"].split("-")
                                y = x[0]
                                desafilia={}
                                desafilia["rut_cliente"] = y
                                headers = {"Content-Type": "application/json; charset=utf-8"}
                                res = requests.post('http://127.0.0.1:5010/ms/certificacion_auto/desafilia_comercio',headers=headers, data = json.dum
ps(desafilia))
                                response = res.json()
                                if "CODIGO_RESPUESTA" in response:
                                        if response["CODIGO_RESPUESTA"]!="1":
                                                error_codigo = '29'
                                                error_mensaje = '[desafilia_comercio] Error, this failed because: ' + str(response["MENSAJE_RESPUESTA
"])
                                        elif response["CODIGO_RESPUESTA"]=="1":
                                                error_codigo = '29'
                                                error_mensaje = '[desafilia_comercio] Problem fixed, the client has been disaffiliated correctly.'
                                        else:
                                                error_codigo = '29'
                                                error_mensaje = '[desafilia_comercio] unknown error.'
                                else:
                                        error_codigo = '26'
                                        error_mensaje = '[desafilia_comercio] Error, Execution failed.'
                        elif error.startswith('CAF: Sin Folios Disponibles') and error.find('No puede emitir Tipo')>0:
                                error_codigo = '30'
                                error_mensaje = 'Problemas al obtener folio. No puede emitir este Tipo de DTE'
                        elif error.startswith('CAF: Sin Folios Disponibles'):
                                error_codigo = '26'
                                error_mensaje = 'Problemas al obtener folio'
                        elif error.startswith('Cliente TBK sin firma centralizada'):
                                error_codigo = '27'
                                error_mensaje = 'Cliente sin firma centralizada'
                                LOG2(s, 'Consigo el payload' + str(payload))
                                x = payload["GE_RUT_EMI"].split("-")
                                y = x[0]
                                desafilia={}
                                desafilia["rut_cliente"] = y
                                headers = {"Content-Type": "application/json; charset=utf-8"}
                                res = requests.post('http://127.0.0.1:5010/ms/certificacion_auto/desafilia_comercio',headers=headers, data = json.dum
ps(desafilia))
                                LOG2(s, 'me caigo despues del ms.')
                                response = res.json()
                                LOG2(s, 'Esto responde el ms: ' + str(res.status_code) + str(response["CODIGO_RESPUESTA"]) + str(type(response)))

                                if "CODIGO_RESPUESTA" in response:
                                    if response["CODIGO_RESPUESTA"]!="1":
                                        error_codigo = '27'
                                        error_mensaje = '[desafilia_comercio] Error, this failed because: ' + str(response["MENSAJE_RESPUESTA"])
                                    elif response["CODIGO_RESPUESTA"]=="1":
                                        error_codigo = '27'
                                        error_mensaje = '[desafilia_comercio] Problem fixed, the client has been disaffiliated correctly.'
                                    else:
                                        error_codigo = '27'
                                        error_mensaje = '[desafilia_comercio] unknown error.'
                                else:
                                    error_codigo = '27'
                                    error_mensaje = '[desafilia_comercio] Error, Execution failed.'
                        elif error.startswith('CAF: Problema GF Falla solicitud de folio'):
                                error_codigo = '28'
                                error_mensaje = 'Problemas al solicitar folio'

                        # FGE - 20220830 - Comentado a petición del cliente
                        # elif error.startswith('El RUT emisor enviado no es valido'):
                        #       error_codigo = '31'
                        #       error_mensaje = 'El RUT emisor enviado no es valido'
                        # elif error.startswith('El RUT emisor enviado no es cliente Transbank'):
                        #        error_codigo = '32'
                        #        error_mensaje = 'El RUT emisor enviado no es cliente Transbank'
                        elif error.isdigit():
                                error_codigo = str(e)
                        else:
                                error_codigo = '99'
                                # error_mensaje = str(e)
                                error_mensaje = 'Error genérico'
                        logger.error('[crea Bolea] Codigo: %s, Mensaje: %s' % (error_codigo, error_mensaje))
                        LOG2(s, 'ERROR: Codigo: %s, Mensaje: %s' % (error_codigo, error_mensaje))
                finally:
                        #json_resp = json.dumps({'GS_CODIGO_RESPUESTA': error_codigo, 'GS_MENSAJE_RESPUESTA': 'VHJhbnNhY2Npw7NuIEV4aXRvc2E=' if error
_codigo == '00' else __encode_string(error_mensaje), 'GS_NUMERO_FOLIO': id_folio, 'GS_ID_UNICO_TBK': id_unico, 'GS_TED': ted})
                        #Grabamos la transaccion
                        if idtx!=-1:
                                try:
                                        if error_mensaje_interno!='':
                                                jtx1 = self.__execute(s,"update "+tabla_trx+" set fecha_termino=now(),estado='"+str(error_codigo)+"',
mensaje='"+error_mensaje_interno+"',folio='"+str(id_folio)+"' where id="+str(idtx),par_bd1)
                                        else:
                                                jtx1 = self.__execute(s,"update "+tabla_trx+" set fecha_termino=now(),estado='"+str(error_codigo)+"',
mensaje='"+error_mensaje+"',folio='"+str(id_folio)+"' where id="+str(idtx),par_bd1)
                                        if jtx1["STATUS"]!="OK":
                                                LOG2(s,str(jtx1))
                                except Exception as e:
                                        LOG2(s,'Falla e='+str(e))
                                        LOG2(s,'Falla update transaccion')
                        LOG2(s, 'error_codigo: %s' % error_codigo)
                        if error_codigo == '00':
                                #return {"status": 200, "message": "ok", "token": _session_id }
                                return self.__responde(s, 'Transaccion Exitosa', {'GS_CODIGO_RESPUESTA': error_codigo, 'GS_MENSAJE_RESPUESTA': 'VHJhb
nNhY2Npw7NuIEV4aXRvc2E=', 'GS_NUMERO_FOLIO': id_folio, 'GS_ID_UNICO_TBK': id_unico, 'GS_TED': ted})
                        else:
                                error_mensaje = self.normalizar_respuesta(error_mensaje)
                                LOG2(s, 'normalizar_respuesta error_mensaje='+error_mensaje)
                                return self.__responde(s, error_mensaje, {'GS_CODIGO_RESPUESTA': error_codigo, 'GS_MENSAJE_RESPUESTA': self.__encode_
string(s, error_mensaje), 'GS_NUMERO_FOLIO': id_folio, 'GS_ID_UNICO_TBK': id_unico, 'GS_TED': ted})


        def normalizar_respuesta(self, s):
                replacements = (("á", "a"),("é", "e"),("í", "i"),("ó", "o"),("ú", "u"),("ñ", "n"))
                for a, b in replacements:
                        s = s.replace(a, b).replace(a.upper(), b.upper())
                return s

        def __responde(self, s, msg, jdata=None, status_code = 200):
                if jdata is None:
                        j={}
                elif isinstance(jdata,dict) is False:
                        j={}
                else:
                        j=jdata

                #DAO 20200311 para poder llamar a ms dentro de otro ms y no __responder el socket
                if hasattr(s,'__responde'):
                        if s.__responde=="NO":
                                LOG2(s,"No repondo al socket, respondo el json")
                                return j

                g = cStringIO.StringIO()
                try:
                        if status_code == 200:
                                LOG2(s, "Responde OK :" + str(msg) + " " + json.dumps(j))
                        else:
                                LOG2(s,"Responde Error :"+str(msg)+" "+json.dumps(j))
                except:
                        pass
                data=json.dumps(j)
                largo=len(data)
                g.write(data)
                g.seek(0)
                s.send_response(status_code)
                s.send_header("Content-type", "application/json")
                s.send_header("Content-Length", str(largo))
                s.end_headers()
                shutil.copyfileobj(g, s.wfile)
                g.close()
                LOG2(s,"FIN")
