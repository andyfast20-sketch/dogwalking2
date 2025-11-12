import os, traceback
print('OPENAI_API_KEY present:', bool(os.environ.get('OPENAI_API_KEY')))
try:
    from openai import OpenAI
    print('Imported OpenAI class OK')
    try:
        client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
        print('Instantiated OpenAI client:', client)
    except Exception as e:
        print('Error instantiating OpenAI client:')
        traceback.print_exc()
except Exception as e:
    print('Error importing OpenAI:')
    import traceback
    traceback.print_exc()
