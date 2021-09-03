from itertools import count, filterfalse
from flask  import Flask ,jsonify,request
import traceback,json,psycopg2
from jsonschema import validate
from datetime import datetime, timedelta
import jwt
import random ,string

app = Flask(__name__)

secret_key='avsvsf123233'

conn = psycopg2.connect(database='local_otocode', user='postgres',host='localhost', password='postgres',port='5433')
cur = conn.cursor()

def store_data(data):
    try:
        #validate the json Data as per The schema
        if not validate_json(json.dumps(data)):
            return jsonify({'message':'Json Data Is Not Valid'}),400

        request_data={}
        #parse and store jsondata in dictionary to use in different cases
        request_data['candidate_name']=data['formData']['full_name']
        request_data['candidate_email']=data['formData']['email']
        request_data['candidate_phone']=data['formData']['mobile_no']
        request_data['custom_field']=str(data['formData'])
        request_data['assessment_id']=data['assessment_id']
        # request_data['email_sent']=data['email_sent']
        # request_data['which_test']=data['which_test']
        # request_data['test_id']=data['test_id']
        request_data['attendee_id']=data['attendee_id']
        # request_data['result']=data['result']
        # request_data['review_status']=data['review_status']
        request_data['test_type']=data['testByQuestions']
        request_data['test_mode']=data['testTypeByUser']
        request_data['token']=data['token']
        request_data['expires']=str(datetime.utcnow()+timedelta(hours=5))

        #validate if name,phone,email is empty string and return
        if request_data['candidate_name']==" ":
            return jsonify ({'message':'Candidate name is blank'}),400
        elif request_data['candidate_email']==" ":
            return jsonify({"message":"Candidate email is blank"}),400
        elif request_data['candidate_phone'] == " ":
            return jsonify ({"message":'candidate phone is blank'}),400
        print(request_data['candidate_email'])
        #call update attendees and assessment results function for private test
        test_type=request_data.get('test_type')
        attendee_id=None
        if request_data['test_mode'].lower()=='public':
            public_new_token=''.join(random.choices(string.ascii_letters+string.digits,k=12))
            request_data['token']=public_new_token
            attendee_id= insert_attendees(request_data,test_type)
        else:
            attendee_id=update_attendees(request_data,test_type)

        if attendee_id!=None:
            request_data['attendee_id']=attendee_id
            jwt_token=jwt.encode(request_data,secret_key,'HS256')
            return jsonify({"access_token":jwt_token}),201
        else:
            return jsonify({'message':'couldnt insert data'}),400
    except:
        print(traceback.print_exc())

def insert_attendees(request_data,test_type):
    try:
        query="""
            insert into attendees(first_name,email_address,token,contact_number,assessment_id,
            custom_field_values,created_at,updated_at)
            values(%s,%s,%s,%s,%s,%s,%s,%s)
            returning id
            """
        dynamic_insert=(
        request_data['candidate_name'],request_data['candidate_email'],request_data['token'],
        request_data['candidate_phone'],request_data['assessment_id'],
        request_data['custom_field'],datetime.utcnow(),
        datetime.utcnow()
        )
        cur.execute(query,dynamic_insert)
        conn.commit()
        insert_result=cur.fetchall()
        count=cur.rowcount
        print(count,"inserted successfully")
        print(insert_result)
        if len(insert_result)> 0:
            attendee_id=insert_result[0][0]
            insert_assessment_results(attendee_id,test_type,request_data['assessment_id'],
            request_data)
            return attendee_id
        else:
            return None
    except:
        print(traceback.print_exc())

def insert_assessment_results(attendee_id,test_type,assessment_id,request_data):
    try:
        table_name="assessment_results"
        if test_type.lower()=="dynamic":
            table_name="dynamic_test_results"
        query="""
            insert into {} (attendee_id,result,review_status,started_at,
            created_at,updated_at,assessment_id)
            values(%s,%s,%s,%s,%s,%s,%s)
            returning id
            """.format(table_name)
        dynamic_insert=(
            request_data['attendee_id'],'before_start',
            'pending',datetime.utcnow(),datetime.utcnow(),datetime.utcnow(),
            request_data['assessment_id']
        )
        cur.execute(query,dynamic_insert)
        conn.commit()
        count=cur.rowcount
        print(count,"inserted assessment_result successfully")
        insert_result=cur.fetchall()
        if len(insert_result)> 0:
            return True
        else:
            return False
    except:
        print(traceback.print_exc())

def update_attendees(request_data,test_type):
    try:
        #query to update attendees for private test(cognitive and dynamic)
        query="""
                update attendees set first_name=%s,
                email_address=%s,contact_number=%s,
                custom_field_values=%s,
                updated_at=%s
                where token=%s and assessment_id=%s
                returning id
            """
        dynamic_values=(request_data['candidate_name'],request_data['candidate_email'],
        request_data['candidate_phone'],request_data['custom_field'],datetime.utcnow(),
        request_data['token'],request_data['assessment_id'])
        cur.execute(query,dynamic_values)
        conn.commit()
        count=cur.rowcount
        print(count,"updated successfully")
        result=cur.fetchall()
        # print(result)
        if len(result)> 0:
            attendee_id=result[0][0]
            print(attendee_id)
            update_assessment_results(attendee_id,test_type,request_data)
            return attendee_id
        else:
            return None
    except:
        print(traceback.print_exc())

def update_assessment_results(id,test_type,request_data):
    try:
        table_name="assessment_results"
        if test_type=="dynamic":
            table_name="dynamic_test_results"
        query="""
            update {} set result='before_start'
            where attendee_id=%s
            returning id
            """.format(table_name)
        dynamic_update=(str('result'),id)
        cur.execute(query,dynamic_update)
        conn.commit()
        count=cur.rowcount
        print(count,"updated assessment_results successfully")
        result=cur.fetchall()
        if result['numberOfRecordsUpdated']> 0:
            return True
        else:
            return False
    except:
        print(traceback.print_exc())

def validate_json(data):
    try:
        schema={
            "type":"object",
            "properties":{
                "assessment_id":{"type":"number"},
                "formData":{
                    "type":"object",
                    "properties":{
                        "full_name":{"type":"string"},
                        "email":{"type":"string"},
                        "mobile_no":{"type":"string"}
                    },
                    "required":["full_name","email","mobile_no"]
                },
                # "email_sent":{"type":"boolean"},
                # "which_test":{"type":"number"},
                # "test_id":{"type":"number"},
                # "attendee_id":{"type":"number"},
                # "result":{"type":"string"},
                # "review_status":{"type":"string"},
                "testByQuestions":{"type":"string"},
                "testTypeByUser":{"type":"string"},
                "token":{"type":"string"}
            },
            "required":["assessment_id","formData","testByQuestions","testTypeByUser","token"]
        }
        json_data=json.loads(data)
        validate(json_data,schema)
        return True
    except:
        print(traceback.print_exc())


