# laive-datalake-poc
# Pasos para crear notebooks locales

Referencias:

https://aws.amazon.com/es/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/
https://medium.com/@guilhermenoronha2001/setting-up-aws-glue-apache-iceberg-locally-on-windows-using-podman-19ec75f0fc81
https://jluis-pcardenas.medium.com/simplificando-el-desarrollo-y-prueba-de-glue-jobs-usando-tu-local-1d2350394629

Entornos WINDOWS:

1. Instalar docker desktop https://www.docker.com/products/docker-desktop/
2. Conectarse a la cuenta de AWS utilizando el Access Key, se coloca los parametros dentro del archivo C:\Users\Caleidos\.aws credentials.
3. Conectarse a este perfil haciendo uso del siguiente comando set AWS_PROFILE 637423333664_AWSAdministratorAccess
4. Al estar dentro probar la conectividad con un caso sencillo por ejemplo listando los buckets de s3 con este comando:
aws s3 ls --profile 637423333664_AWSAdministratorAccess
5. En este punto ya debemos tener habilitado Docker, pero antes debemos cambiar a contenedores de linux. Debemos ir al icono de Docker en la parte inferior del escritorio y seleccionar "Cambiar a contenedores de Linux".
6. Todos los pasos los podemos hacer por PowerShell, debemos ejecutar este comando docker pull amazon/aws-glue-libs:glue_libs_3.0.0_image_01
7.Luego debemos hacer la ejecución del siguiente contenedor 
docker run -it -v C:/Users/Caleidos/.aws:/home/glue_user/.aws -v C:/Users/Caleidos/laive-datalake-poc:/home/glue_user/workspace/jupyter_workspace/ -e AWS_PROFILE=$PROFILE_NAME -e AWS_REGION=us-east-2 -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh

Considerar que los volumenes se asignan al contenedor de Docker.

# Nota:
en los buckets donde se encuentran los objetos (data), considerar la siguiente política de bucket (ejemplo):
{
    "Version": "2012-10-17",
    "Id": "ExamplePolicy01",
    "Statement": [
        {
            "Sid": "ExampleStatement01",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::delta-glue-tests-2/*",
                "arn:aws:s3:::delta-glue-tests-2"
            ]
        }
    ]
}